package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for lzop compressed files. This class
 * handles the nudging the input splits onto LZO boundaries using the existing LZO index files.
 * Subclass and implement getRecordReader to define custom LZO-based input formats.
 */
public abstract class LzoInputFormat<K, V> extends FileInputFormat<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoInputFormat.class);

  private final Map<Path, LzoIndex> indexes_ = new HashMap<Path, LzoIndex>();

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    // The list of files is no different.
    List<FileStatus> files = super.listStatus(job);

    // To help split the files at LZO boundaries, walk the list of lzo files and, if they
    // have an associated index file, save that for later.
    String fileExtension = new LzopCodec().getDefaultExtension();

    Iterator<FileStatus> it = files.iterator();
    while (it.hasNext()) {
      FileStatus fileStatus = it.next();
      Path file = fileStatus.getPath();

      if (!file.toString().endsWith(fileExtension)) {
        // Get rid of non lzo files.
        it.remove();
      } else {
        // Read the index file.
        LzoIndex index = LzoIndex.readIndex(file.getFileSystem(job.getConfiguration()), file);
        indexes_.put(file, index);
      }
    }

    return files;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // LZO files are splittable precisely when they have an associated index file.
    LzoIndex index = indexes_.get(filename);
    return !index.isEmpty();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> defaultSplits = super.getSplits(job);

    // Find new starts and ends of the file splits that align with the lzo blocks.
    List<InputSplit> result = new ArrayList<InputSplit>();

    for (InputSplit genericSplit : defaultSplits) {
      // Load the index.
      FileSplit fileSplit = (FileSplit)genericSplit;
      Path file = fileSplit.getPath();
      LzoIndex index = indexes_.get(file);
      if (index == null) {
        // In listStatus above, a (possibly empty, but non-null) index was put in for every split.
        throw new IOException("Index not found for " + file);
      }

      if (index.isEmpty()) {
        // Empty index, so leave the default split.
        result.add(fileSplit);
        continue;
      }

      long start = fileSplit.getStart();
      long end = start + fileSplit.getLength();

      long lzoStart = index.alignSliceStartToIndex(start, end);
      long lzoEnd = index.alignSliceEndToIndex(end, file.getFileSystem(job.getConfiguration()).getFileStatus(file).getLen());

      if (lzoStart != LzoIndex.NOT_FOUND  && lzoEnd != LzoIndex.NOT_FOUND) {
        result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart, fileSplit.getLocations()));
        LOG.info("Added LZO split for " + file + "[start=" + lzoStart + ", length=" + (lzoEnd - lzoStart) + "]");
      }
    }

    return result;
  }
}
