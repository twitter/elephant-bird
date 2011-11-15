package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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

  private static final String lzoIndexSuffix = ".lzo" + LzoIndex.LZO_INDEX_SUFFIX;
  private static final String lzoTmpIndexSuffix = ".lzo" + LzoIndex.LZO_TMP_INDEX_SUFFIX;

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
      return !name.startsWith(".") &&
             !name.startsWith("_") &&
             name.endsWith(".lzo");
    }};

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    // The list of files is no different.
    List<FileStatus> files = super.listStatus(job);
    List<FileStatus> results = Lists.newArrayList();
    boolean recursive = job.getConfiguration().getBoolean("mapred.input.dir.recursive", false);
    boolean includeNonLzo = job.getConfiguration().getBoolean("elephantbird.lzo.input.include.nonlzo", false);
    Iterator<FileStatus> it = files.iterator();
    while (it.hasNext()) {
      FileStatus fileStatus = it.next();
      FileSystem fs = fileStatus.getPath().getFileSystem(job.getConfiguration());
      addInputPath(results, fs, fileStatus, recursive, includeNonLzo);
    }

    LOG.debug("Total lzo input paths to process : " + results.size());
    return results;
  }

  //MAPREDUCE-1501
  /**
   * Add lzo file(s). If recursive is set, traverses the directories.
   * @param result
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
                 FileStatus pathStat, boolean recursive, boolean includeNonLzo) throws IOException {
    Path path = pathStat.getPath();
    if (pathStat.isDir()) {
      if (recursive) {
        for(FileStatus stat: fs.listStatus(path, hiddenPathFilter)) {
          addInputPath(results, fs, stat, recursive, includeNonLzo);
        }
      }
    } else {
      if (includeNonLzo) {
        String name = path.getName();
        if (!name.endsWith(lzoIndexSuffix) && !name.endsWith(lzoTmpIndexSuffix)) {
          results.add(pathStat);
        }
      } else { // read only lzo files
        if (visibleLzoFilter.accept(path)) {
          results.add(pathStat);
        }
      }
    }
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    /* This should ideally return 'false'
     * and splitting should be handled completely in
     * this.getSplit(). Right now, FileInputFormat splits across the
     * blocks and this.getSplits() adjusts the positions.
     */
    try {
      if (visibleLzoFilter.accept(filename)) {
        FileSystem fs = filename.getFileSystem( context.getConfiguration() );
        return fs.exists( filename.suffix( LzoIndex.LZO_INDEX_SUFFIX ) );
      } else { // non-lzo file
        //only simple files are splitable (borrowed from TextInputFormat).
        CompressionCodec codec =
          new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        return codec == null;
        // TODO: bzip2 files are splitable. reuse reader from PigStorage.
      }
    } catch (IOException e) { // not expected
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> defaultSplits = super.getSplits(job);

    // Find new starts and ends of the file splits that align with the lzo blocks.
    List<InputSplit> result = new ArrayList<InputSplit>();

    Path prevFile = null;
    LzoIndex prevIndex = null;

    for (InputSplit genericSplit : defaultSplits) {
      // Load the index.
      FileSplit fileSplit = (FileSplit)genericSplit;
      Path file = fileSplit.getPath();

      if (!visibleLzoFilter.accept(file)) {
        // non lzo file. just keep the split
        result.add(fileSplit);
        continue;
      }

      LzoIndex index; // reuse index for files with multiple blocks.
      if ( file.equals(prevFile) ) {
        index = prevIndex;
      } else {
        index = LzoIndex.readIndex(file.getFileSystem(job.getConfiguration()), file);
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
      long lzoEnd = index.alignSliceEndToIndex(end, file.getFileSystem(job.getConfiguration()).getFileStatus(file).getLen());

      if (lzoStart != LzoIndex.NOT_FOUND  && lzoEnd != LzoIndex.NOT_FOUND) {
        result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart, fileSplit.getLocations()));
        LOG.debug("Added LZO split for " + file + "[start=" + lzoStart + ", length=" + (lzoEnd - lzoStart) + "]");
      }
      // else ignore the data?
      // should handle splitting the entire file here so that
      // such errors can be handled better.
    }

    return result;
  }
}
