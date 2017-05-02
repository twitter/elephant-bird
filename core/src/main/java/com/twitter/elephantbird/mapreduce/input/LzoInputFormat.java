package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.LzoIndex;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for lzop compressed files. This class
 * handles the nudging the input splits onto LZO boundaries using the existing LZO index files.
 * Subclass and implement getRecordReader to define custom LZO-based input formats.<p>
 * <b>Note:</b> unlike the stock FileInputFormat, this recursively examines directories for matching files.
 */
public abstract class LzoInputFormat<K, V> extends FileInputFormat<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoInputFormat.class);

  private final PathFilter hiddenPathFilter = new PathFilter() {
    // avoid hidden files and directories.
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith(".") &&
             !name.startsWith("_");
    }
  };

  private final PathFilter visibleLzoFilter = new PathFilter() {
    //applies to lzo files
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return hiddenPathFilter.accept(path) &&
             name.endsWith(".lzo");
    }};

  private final PathFilter lzoIndexFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      final String name = path.getName();
      return hiddenPathFilter.accept(path) &&
             name.endsWith(LzoIndex.LZO_INDEX_SUFFIX);
    }
  };

  private static class LzoSplitStatus {
    private FileStatus lzoFileStatus;
    private FileStatus lzoIndexFileStatus;

    @Override
    public String toString() {
      return LzoSplitStatus.class.getName() + "[ lzo=" + lzoFileStatus
          + " index=" + lzoIndexFileStatus + " ]";
    }
  }

  private final Map<Path,LzoSplitStatus> splitStatusMap
      = new HashMap<Path,LzoSplitStatus>();

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    // The list of files is no different.
    List<FileStatus> files = super.listStatus(job);
    List<FileStatus> results = Lists.newArrayList();
    boolean recursive = HadoopCompat.getConfiguration(job).getBoolean("mapred.input.dir.recursive", false);
    Iterator<FileStatus> it = files.iterator();
    while (it.hasNext()) {
      FileStatus fileStatus = it.next();
      FileSystem fs = fileStatus.getPath().getFileSystem(HadoopCompat.getConfiguration(job));
      addInputPath(results, fs, fileStatus, recursive);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total lzo input paths to process : " + results.size());
    }
    return results;
  }

  //MAPREDUCE-1501
  /**
   * Add lzo file(s). If recursive is set, traverses the directories.
   * @param results
   *          The List to store all files.
   * @param fs
   *          The FileSystem.
   * @param pathStat
   *          The input path.
   * @param recursive
   *          Traverse in to directory
   * @throws IOException
   */
  protected void addInputPath(List<FileStatus> results, FileSystem fs,
                 FileStatus pathStat, boolean recursive) throws IOException {
    Path path = pathStat.getPath();
    if (pathStat.isDir()) {
      if (recursive) {
        for(FileStatus stat: fs.listStatus(path, hiddenPathFilter)) {
          addInputPath(results, fs, stat, recursive);
        }
      }
    } else if ( visibleLzoFilter.accept(path) ) {
      results.add(pathStat);
      lzoSplitStatus(path).lzoFileStatus = pathStat;
    } else if (lzoIndexFilter.accept(path)) {
      final String lzoIndexName = path.getName();
      final String lzoName = lzoIndexName.substring(0, lzoIndexName.length() -
          LzoIndex.LZO_INDEX_SUFFIX.length());
      final Path lzoPath = new Path(path.getParent(), lzoName);
      lzoSplitStatus(lzoPath).lzoIndexFileStatus = pathStat;
    }
  }

  private LzoSplitStatus lzoSplitStatus(Path path) {
    LzoSplitStatus lzoSplitStatus = splitStatusMap.get(path);
    if (lzoSplitStatus == null) {
      lzoSplitStatus = new LzoSplitStatus();
      splitStatusMap.put(path, lzoSplitStatus);
    }
    return lzoSplitStatus;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    /* This should ideally return 'false'
     * and splitting should be handled completely in
     * this.getSplit(). Right now, FileInputFormat splits across the
     * blocks and this.getSplits() adjusts the positions.
     */
    LzoSplitStatus lzoSplitStatus = splitStatusMap.get(filename);
    return lzoSplitStatus != null && lzoSplitStatus.lzoIndexFileStatus != null;
  }

  private List<InputSplit> getSplitsInternal(JobContext job)
      throws IOException {
    List<InputSplit> defaultSplits = super.getSplits(job);

    // Find new starts and ends of the file splits that align with the lzo blocks.
    List<InputSplit> result = new ArrayList<InputSplit>();

    Path prevFile = null;
    LzoIndex prevIndex = null;

    for (InputSplit genericSplit : defaultSplits) {
      // Load the index.
      FileSplit fileSplit = (FileSplit)genericSplit;
      Path file = fileSplit.getPath();

      LzoIndex index; // reuse index for files with multiple blocks.
      final LzoSplitStatus lzoSplitStatus = splitStatusMap.get(file);
      if ( file.equals(prevFile) ) {
        index = prevIndex;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cached file status " + lzoSplitStatus);
        }
        final FileStatus indexFileStatus = lzoSplitStatus.lzoIndexFileStatus;
        index = indexFileStatus == null
            ? new LzoIndex()
            : LzoIndex.readIndex(
                  file.getFileSystem(HadoopCompat.getConfiguration(job)), file);
        prevFile = file;
        prevIndex = index;
      }

      if (index == null) {
        // In listStatus above, a (possibly empty, but non-null) index was put in for every split.
        throw new IOException("Index not found for " + file);
      }

      if (index.isEmpty()) {
        // Empty index, so leave the default split.
        // split's start position should be 0.
        result.add(fileSplit);
        continue;
      }

      long start = fileSplit.getStart();
      long end = start + fileSplit.getLength();

      long lzoStart = index.alignSliceStartToIndex(start, end);
      long lzoEnd = index.alignSliceEndToIndex(end,
          lzoSplitStatus.lzoFileStatus.getLen());

      if (lzoStart != LzoIndex.NOT_FOUND  && lzoEnd != LzoIndex.NOT_FOUND) {
        result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart, fileSplit.getLocations()));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added LZO split for " + file + "[start=" + lzoStart + ", length=" + (lzoEnd - lzoStart) + "]");
        }
      }
      // else ignore the data?
      // should handle splitting the entire file here so that
      // such errors can be handled better.
    }

    return result;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    try {
      return getSplitsInternal(job);
    } finally {
      splitStatusMap.clear(); // no use beyond getSplits
    }
  }
}
