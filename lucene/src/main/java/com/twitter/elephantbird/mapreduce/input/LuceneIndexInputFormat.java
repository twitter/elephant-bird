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
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import com.twitter.elephantbird.util.HdfsUtils;

/**
 * <p>
 *    Base class for input formats that read lucene indexes stored in HDFS directories.
 *    Given a list of indexes and queries, runs each query over each index. Implements split
 *    combining (combines multiple indexes into one split) based on
 *    the total size of the index directory and the configured max combined split size.
 * </p>
 * <p>
 *    Emits key, value records where key is the query that resulted in value
 *    (key is is actually the position in the list of queries, not the query string itself)
 * </p>
 * <p>
 *    Subclasses must provide:
 *    <ul>
 *      a {@link PathFilter} for identifying HDFS
 *      directories that contain lucene indexes
 *    </ul>
 *    <ul>
 *      a {@link LuceneIndexRecordReader} which describes how to convert a String into a
 *      Query and how to convert a Document into a value of type T
 *    </ul>
 * </p>
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

  public static final String MAX_COMBINE_SPLIT_SIZE_KEY =
    LuceneIndexInputFormat.class.getCanonicalName() + ".maxcombinesplitsize";

  // default to 1GB
  // TODO: choose a reasonable default
  private static final long DEFAULT_MAX_COMBINE_SPLIT_SIZE_BYTES = 1024*1024*1024L;

  private static final String[] EMPTY_NODE_ARRAY = new String[0];

  private Path[] inputPaths = null;
  private PathFilter indexDirPathFilter = null;
  private long maxCombineSplitSizeBytes;

  /**
   * Subclasses must provide a {@link PathFilter} for identifying HDFS
   * directories that contain lucene indexes. When directories are being
   * searched recursively for index directories, this path filter will be used
   * to determine if a directory is a lucene index.
   *
   * @param conf job conf
   * @return a path filter that accepts directories with lucene indexes in them
   * @throws IOException
   */
  public abstract PathFilter getIndexDirPathFilter(Configuration conf) throws IOException;

  @VisibleForTesting
  void loadConfig(Configuration conf) throws IOException {
    inputPaths = getInputPaths(conf);

    indexDirPathFilter = Preconditions.checkNotNull(getIndexDirPathFilter(conf),
      "You must provide a non-null PathFilter");

    maxCombineSplitSizeBytes = Preconditions.checkNotNull(getMaxCombineSplitSizeBytes(conf),
      "max combine split size cannot be null");
  }

  /**
   * <p>
   * Creates splits with multiple indexes per split
   * (if they are smaller than maxCombineSplitSizeBytes).
   * It is possible for a split to be larger than maxCombineSplitSizeBytes,
   * if it consists of a single index that is
   * larger than maxCombineSplitSizeBytes.
   * </p>
   * <p>All inputPaths will be searched for indexes recursively</p>
   * <p>
   *   The bin-packing problem of combining splits is solved naively:
   *   <ol>
   *     <li>Sort all indexes by size</li>
   *     <li>Begin packing indexes into splits until adding the next split would cause the split to
   *         exceed maxCombineSplitSizeBytes</li>
   *     <li>Begin packing subsequent indexes into the next split, and so on</li>
   *   </ol>
   * </p>
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {

    // load settings from job conf
    loadConfig(job.getConfiguration());

    // find all the index dirs and create a split for each
    PriorityQueue<LuceneIndexInputSplit> splits = findSplits(job.getConfiguration());

    // combine the splits based on maxCombineSplitSize
    List<InputSplit> combinedSplits = combineSplits(splits, maxCombineSplitSizeBytes);

    return combinedSplits;
  }

  @VisibleForTesting
  PriorityQueue<LuceneIndexInputSplit> findSplits(Configuration conf) throws IOException {
    PriorityQueue<LuceneIndexInputSplit> splits = new PriorityQueue<LuceneIndexInputSplit>();
    List<Path> indexDirs = Lists.newLinkedList();

    // find all indexes nested under all the input paths
    // (which happen to be directories themselves)
    for (Path path : inputPaths) {
      HdfsUtils.collectPaths(path, indexDirPathFilter, conf, true, indexDirs);
    }

    // compute the size of each index
    // and create a single split per index
    for (Path indexDir : indexDirs) {
      long size = HdfsUtils.getDirectorySize(indexDir, conf);
      splits.add(new LuceneIndexInputSplit(Lists.newLinkedList(Arrays.asList(indexDir)), size));
    }
    return splits;
  }

  @VisibleForTesting
  static List<InputSplit> combineSplits(PriorityQueue<LuceneIndexInputSplit> splits, long maxSize) {
    // now take the one-split-per-index splits and combine them into multi-index-per-split splits
    List<InputSplit> combinedSplits = Lists.newLinkedList();
    LuceneIndexInputSplit currentSplit = splits.poll();
    while (currentSplit != null) {
      while (currentSplit.getLength() < maxSize) {
        LuceneIndexInputSplit nextSplit = splits.peek();
        if (nextSplit == null) {
          break;
        }
        if (currentSplit.getLength() + nextSplit.getLength() > maxSize) {
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
    conf.set(QUERIES_KEY, JSONArray.toJSONString(queries));
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
    String queries = Preconditions.checkNotNull(conf.get(QUERIES_KEY),
        "You must call LuceneIndexInputFormat.setQueries()");
    return Lists.<String>newArrayList(((JSONArray) JSONValue.parse(queries)));
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
   * Set the max size of a combined split. Splits can be larger than size if they contain
   * a single index which is larger than size, but if they contain multiple indexes they will
   * always be less than or equal to size
   *
   * @param size max size for combined splits in bytes
   * @param conf job conf
   */
  public static void setMaxCombineSplitSizeBytes(long size, Configuration conf) {
    conf.setLong(MAX_COMBINE_SPLIT_SIZE_KEY, size);
  }

  /**
   * Get the max size of a combined split in bytes
   * @param conf job conf
   * @return the max size of a combined split in bytes
   */
  public static long getMaxCombineSplitSizeBytes(Configuration conf) {
    return conf.getLong(MAX_COMBINE_SPLIT_SIZE_KEY, DEFAULT_MAX_COMBINE_SPLIT_SIZE_BYTES);
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
     * Because an index consists of multiple files there's not much to be gained from
     * finding nodes where there is locality
     * TODO: is that a valid assumption? Should we return locality of the large files instead?
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
