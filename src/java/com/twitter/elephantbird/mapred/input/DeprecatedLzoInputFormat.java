package com.twitter.elephantbird.mapred.input;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * This class serves as a base class for lzo input formats based on
 * the old (org.apache.hadoop.mapred.*) hadoop API style
 * which is deprecated but still required in places.  Streaming, for example,
 * does a check that the given input format is a descendant of
 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
 * from the new API fails.  In order for streaming to work, you must use
 * com.twitter.elephantbird.mapred.input.DeprecatedLzoTextInputFormat, not
 * com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat.  The classes attempt to be alike in
 * every other respect.
 */

@SuppressWarnings("deprecation")
public abstract class DeprecatedLzoInputFormat<K, V> extends FileInputFormat<K, V> {

  @Override
  protected FileStatus[] listStatus(JobConf conf) throws IOException {
    List<FileStatus> files = new ArrayList<FileStatus>(Arrays.asList(super.listStatus(conf)));

    String fileExtension = new LzopCodec().getDefaultExtension();

    Iterator<FileStatus> it = files.iterator();
    while (it.hasNext()) {
      FileStatus fileStatus = it.next();
      Path file = fileStatus.getPath();

      if (!file.toString().endsWith(fileExtension)) {
        // Get rid of non-LZO files.
        it.remove();
      }
    }

    return files.toArray(new FileStatus[]{});
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    try {
      return fs.exists(filename.suffix(LzoIndex.LZO_INDEX_SUFFIX));
    } catch (IOException e) { // not expected
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    FileSplit[] splits = (FileSplit[]) super.getSplits(conf, numSplits);

    // Find new starts/ends of the filesplit that align with the LZO blocks.
    List<FileSplit> result = new ArrayList<FileSplit>();

    Path prevFile = null;
    LzoIndex prevIndex = null;

    for (FileSplit fileSplit : splits) {
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);

      LzoIndex index; // reuse index for files with multiple blocks.
      if (file.equals(prevFile)) {
        index = prevIndex;
      } else {
        index = LzoIndex.readIndex(fs, file);
        prevFile = file;
        prevIndex = index;
      }

      if (index == null) {
        // Each LZO file gets a (possibly empty) index in the map, so this shouldn't happen.
        throw new IOException("Index not found for " + file);
      }
      if (index.isEmpty()) {
        // Empty index, so nothing we can do. This split is a full-file split.
        result.add(fileSplit);
        continue;
      }

      long start = fileSplit.getStart();
      long end = start + fileSplit.getLength();

      // Realign the split start and end on LZO block boundaries.
      long lzoStart = index.alignSliceStartToIndex(start, end);
      long lzoEnd = index.alignSliceEndToIndex(end, fs.getFileStatus(file).getLen());

      if (lzoStart != LzoIndex.NOT_FOUND && lzoEnd != LzoIndex.NOT_FOUND) {
        result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart, fileSplit.getLocations()));
      }
    }

    return result.toArray(new FileSplit[result.size()]);
  }
}
