package com.twitter.elephantbird.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.twitter.elephantbird.mapreduce.output.LuceneIndexOutputFormat;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.HdfsUtils;

/**
 * Base class for input formats that read lucene indexes stored in HDFS directories.
 * Given a list of indexes and queries, runs each query over each index. Implements split
 * combining (combines multiple indexes into one split) based on
 * the total size of the index directory and the configured max combined split size.
 * <p>
 * Emits key, value records where key is the query that resulted in value
 * (key is actually the position in the list of queries, not the query string itself)
 * <p>
 * Subclasses must provide:
 * <ul>
 *  <li>a {@link LuceneIndexRecordReader} which describes how to convert a String into a
 *      Query and how to convert a Document into a value of type T</li>
 * </ul>
 * Subclasses may provide:d
 * <ul>
 *   <li> a {@link PathFilter} for identifying HDFS directories that contain lucene indexes</li>
 * </ul>
 *
 * @param <T> - the type that your lucene Documents will be converted to
 * @author Alex Levenson
 */
public abstract class LuceneIndexInputFormat<T extends Writable>
    extends InputFormat<IntWritable, T> {

  public static final String QUERIES_KEY = LuceneIndexInputFormat.class.getCanonicalName()
    + ".queries";

  public static final String INPUT_PATHS_KEY = LuceneIndexInputFormat.class.getCanonicalName()
    + ".inputpaths";

  public static final String MAX_NUM_INDEXES_PER_SPLIT_KEY =
    LuceneIndexInputFormat.class.getCanonicalName() + ".max_num_indexes_per_split";

  // Empirically it seems that 200 is a reasonable number of small
  // indexes for one mapper to process in a few minutes, and thousands of
  // of small indexes can take one mapper more than 30 minutes to process
  // due to overhead for each index.
  private static final long DEFAULT_MAX_NUM_INDEXES_PER_SPLIT = 200;

  public static final String MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY =
    LuceneIndexInputFormat.class.getCanonicalName() + ".max_combined_index_size_per_split";

  // default to 10GB
  // back of the envelope reasoning:
  // Assume 1 mapper should process 1 GB, and each index will return 1/10th its size in records
  private static final long DEFAULT_MAX_COMBINED_INDEX_SIZE_PER_SPLIT = 10*1024*1024*1024L;

  private static final String[] EMPTY_NODE_ARRAY = new String[0];

  private Path[] inputPaths = null;
  private PathFilter indexDirPathFilter = null;
  private long maxCombinedIndexSizePerSplit;
  private long maxNumIndexesPerSplit;

  /**
   * Subclasses may provide a {@link PathFilter} for identifying HDFS
   * directories that contain lucene indexes. When directories are being
   * searched recursively for index directories, this path filter will be used
   * to determine if a directory is a lucene index.
   * <p>
   * The default is to treat any directory whose name begins with "-index" as a lucene index,
   * which matches what {@link LuceneIndexOutputFormat} generates.
   *
   * @param conf job conf
   * @return a path filter that accepts directories with lucene indexes in them
   * @throws IOException
   */
  public PathFilter getIndexDirPathFilter(Configuration conf) throws IOException {
    return LuceneIndexOutputFormat.newIndexDirFilter(conf);
  }

  @VisibleForTesting
  void loadConfig(Configuration conf) throws IOException {
    inputPaths = getInputPaths(conf);

    indexDirPathFilter = Preconditions.checkNotNull(getIndexDirPathFilter(conf),
      "You must provide a non-null PathFilter");

    maxCombinedIndexSizePerSplit = Preconditions.checkNotNull(
        getMaxCombinedIndexSizePerSplit(conf),
        MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY + " cannot be null");

    maxNumIndexesPerSplit = Preconditions.checkNotNull(getMaxNumIndexesPerSplit(conf),
        MAX_NUM_INDEXES_PER_SPLIT_KEY + " cannot be null");
  }

  /**
   * Creates splits with multiple indexes per split
   * (if they are smaller than maxCombinedIndexSizePerSplit).
   * It is possible for a split to be larger than maxCombinedIndexSizePerSplit,
   * if it consists of a single index that is
   * larger than maxCombinedIndexSizePerSplit.
   * <p>
   * All inputPaths will be searched for indexes recursively
   * <p>
   * The bin-packing problem of combining splits is solved naively:
   * <ol>
   *   <li>Sort all indexes by size</li>
   *   <li>Begin packing indexes into splits until adding the next split would cause the split to
   *       exceed maxCombinedIndexSizePerSplit</li>
   *   <li>Begin packing subsequent indexes into the next split, and so on</li>
   * </ol>
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {

    // load settings from job conf
    loadConfig(HadoopCompat.getConfiguration(job));

    // find all the index dirs and create a split for each
    PriorityQueue<LuceneIndexInputSplit> splits = findSplits(HadoopCompat.getConfiguration(job));

    // combine the splits based on maxCombineSplitSize
    List<InputSplit> combinedSplits = combineSplits(splits, maxCombinedIndexSizePerSplit,
      maxNumIndexesPerSplit);

    return combinedSplits;
  }

  /**
   * Finds and creates all the index splits based on the input paths set in conf
   * @param conf job conf
   * @return a priority queue of the splits, default is sorted by size
   * @throws IOException
   */
  protected PriorityQueue<LuceneIndexInputSplit> findSplits(Configuration conf) throws IOException {
    PriorityQueue<LuceneIndexInputSplit> splits = new PriorityQueue<LuceneIndexInputSplit>();
    List<Path> indexDirs = Lists.newLinkedList();

    // find all indexes nested under all the input paths
    // (which happen to be directories themselves)
    for (Path path : inputPaths) {
      HdfsUtils.collectPaths(path, path.getFileSystem(conf), indexDirPathFilter, indexDirs);
    }

    // compute the size of each index
    // and create a single split per index
    for (Path indexDir : indexDirs) {
      long size = HdfsUtils.getDirectorySize(indexDir, indexDir.getFileSystem(conf));
      splits.add(new LuceneIndexInputSplit(Lists.newLinkedList(Arrays.asList(indexDir)), size));
    }
    return splits;
  }

  protected List<InputSplit> combineSplits(PriorityQueue<LuceneIndexInputSplit> splits,
                                           long maxCombinedIndexSizePerSplit,
                                           long maxNumIndexesPerSplit) {

    // now take the one-split-per-index splits and combine them into multi-index-per-split splits
    List<InputSplit> combinedSplits = Lists.newLinkedList();
    LuceneIndexInputSplit currentSplit = splits.poll();
    while (currentSplit != null) {
      while (currentSplit.getLength() < maxCombinedIndexSizePerSplit) {
        LuceneIndexInputSplit nextSplit = splits.peek();
        if (nextSplit == null) {
          break;
        }
        if (currentSplit.getLength() + nextSplit.getLength() > maxCombinedIndexSizePerSplit) {
          break;
        }
        if (currentSplit.getIndexDirs().size() >= maxNumIndexesPerSplit) {
          break;
        }
        currentSplit.combine(nextSplit);
        splits.poll();
      }
      combinedSplits.add(currentSplit);
      currentSplit = splits.poll();
    }
    return combinedSplits;
  }

  /**
   * Set the queries to run over the indexes
   * These can be Strings suitable for parsing with a QueryParser, or they can be
   * a custom serialized Query object. They have to be Strings so that they can be
   * written to the job conf. They will be deserialized / parsed by the abstract
   * method {@link LuceneIndexRecordReader#deserializeQuery(String)}
   *
   * @param queries queries to run over the indexes
   * @param conf job conf
   * @throws IOException
   */
  public static void setQueries(List<String> queries, Configuration conf) throws IOException {
    Preconditions.checkNotNull(queries);
    Preconditions.checkArgument(!queries.isEmpty());
    HadoopUtils.writeStringListToConfAsBase64(QUERIES_KEY, queries, conf);
  }

  /**
   * Get the queries to run over the indexes
   * These are the queries in the same form as they were passed to {@link #setQueries} above
   *
   * @param conf job conf
   * @return queries as passed to {@link #setQueries}
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static List<String> getQueries(Configuration conf) throws IOException {
    return Preconditions.checkNotNull(HadoopUtils.readStringListFromConfAsBase64(QUERIES_KEY, conf),
      "You must call LuceneIndexInputFormat.setQueries()");
  }

  /**
   * Check whether queries have been set yet for this job, useful for lazy loading
   * queries into the config
   *
   * @param conf job conf
   * @return whether the queries have been set yet
   */
  public static boolean queriesSet(Configuration conf) {
    return conf.get(QUERIES_KEY) != null;
  }

  /**
   * Sets the input paths for for this input format.
   * All paths will be searched for indexes recursively
   *
   * @param paths the input paths
   * @param conf the job conf
   * @throws IOException
   */
  public static void setInputPaths(List<Path> paths, Configuration conf) throws IOException {
    Preconditions.checkNotNull(paths);
    Preconditions.checkArgument(!paths.isEmpty());
    String[] pathStrs = new String[paths.size()];
    int i = 0;
    for (Path p : paths) {
      FileSystem fs = p.getFileSystem(conf);
      pathStrs[i++] = fs.makeQualified(p).toString();
    }
    conf.setStrings(INPUT_PATHS_KEY, pathStrs);
  }

  /**
   * Gets the input paths for this input format
   *
   * @param conf the job conf
   */
  public static Path[] getInputPaths(Configuration conf) {
    String[] pathStrs = Preconditions.checkNotNull(conf.getStrings(INPUT_PATHS_KEY),
        "You must call LuceneIndexInputFormat.setInputPaths()");
    Path[] paths = new Path[pathStrs.length];
    for (int i = 0; i < pathStrs.length; i++) {
      paths[i] = new Path(pathStrs[i]);
    }
    return paths;
  }

  /**
   * Set the max combined size of indexes to be processed by one split.
   *
   * If an index is larger than size than it will be put in its own split, but all splits
   * containing multiple indexes will have a combined size <= size.
   *
   * @param size the max combined size of indexes to be processed by one split in bytes
   * @param conf job conf
   */
  public static void setMaxCombinedIndexSizePerSplitBytes(long size, Configuration conf) {
    conf.setLong(MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY, size);
  }

  /**
   * Get the max size of a combined split in bytes
   * @param conf job conf
   * @return the max size of a combined split in bytes
   */
  public static long getMaxCombinedIndexSizePerSplit(Configuration conf) {
    return conf.getLong(MAX_COMBINED_INDEX_SIZE_PER_SPLIT_KEY,
      DEFAULT_MAX_COMBINED_INDEX_SIZE_PER_SPLIT);
  }

  /**
   * Set the max number of indexes to process for a single split.
   *
   * If a combined split still has room for more indexes (as determined by
   * {@link #getMaxCombinedIndexSizePerSplit}) then more indexes will be added to it
   * UNLESS that would cause the split to have more than num indexes combined into it.
   *
   * This helps preventing one split from getting thousands of small indexes which can make it
   * significantly slower than the others.
   *
   * @param num max number of splits per combined split
   * @param conf job conf
   */
  public static void setMaxNumIndexesPerSplit(long num, Configuration conf) {
    conf.setLong(MAX_NUM_INDEXES_PER_SPLIT_KEY, num);
  }

  /**
   * Get the max number of indexes per split
   *
   * @param conf job conf
   * @return the max number of indexes per split
   */
  public static long getMaxNumIndexesPerSplit(Configuration conf) {
    return conf.getLong(MAX_NUM_INDEXES_PER_SPLIT_KEY, DEFAULT_MAX_NUM_INDEXES_PER_SPLIT);
  }

  /**
   * A split that represents multiple index (directories).
   * Has to be {@link Writable} to work with pig
   */
  public static class LuceneIndexInputSplit extends InputSplit
                                            implements Writable, Comparable<LuceneIndexInputSplit> {
    private List<Path> indexDirs;
    private Long length;

    /**
     * Required for instantiation by reflection
     */
    public LuceneIndexInputSplit() { }

    public LuceneIndexInputSplit(List<Path> indexDirs, long length) {
      this.indexDirs = indexDirs;
      this.length = length;
    }

    /**
     * Merge other into this split
     * Will have no effect on other
     * @param other the split to combine
     */
    public void combine(LuceneIndexInputSplit other) {
      indexDirs.addAll(other.getIndexDirs());
      length += other.getLength();
    }

    /**
     * Get the size of this split in bytes
     * @return the size of this split in bytes
     */
    @Override
    public long getLength() {
      return length;
    }

    /**
     * Because an index consists of multiple (multi-block) files there's not much to be gained from
     * finding nodes where there is locality
     * @return an empty String[]
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return EMPTY_NODE_ARRAY;
    }

    /**
     * @return the list of indexes in this split (which are directories)
     */
    public List<Path> getIndexDirs() {
      return indexDirs;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(length);
      out.writeInt(indexDirs.size());
      for(Path p : indexDirs) {
        Text.writeString(out, p.toString());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.length = in.readLong();
      int numDirs = in.readInt();
      this.indexDirs = Lists.newLinkedList();
      for (int i = 0; i < numDirs; i++) {
        String path = Text.readString(in);
        this.indexDirs.add(new Path(path));
      }
    }

    /**
     * sorts by length (size in bytes)
     */
    @Override
    public int compareTo(LuceneIndexInputSplit other) {
      return length.compareTo(other.getLength());
    }

    @Override
    public String toString() {
      return "LuceneIndexInputSplit<indexDirs:" + indexDirs.toString() + " length:" + length + ">";
    }
  }
}
