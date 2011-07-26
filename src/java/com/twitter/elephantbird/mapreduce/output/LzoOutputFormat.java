package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Base class for Lzo outputformats.
 * provides an helper method to create lzo output stream.
 */
public abstract class LzoOutputFormat<K, V> extends FileOutputFormat<K, V> {

  public static final Logger LOG = LogManager.getLogger(LzoOutputFormat.class);

  /**
   * A work-around to support environments with older versions of LzopCodec.
   * It might not be feasible for to select right version of hadoop-lzo
   * in some cases. This should be removed latest by EB-3.0.
   */
  private static boolean isLzopIndexSupported = false;
  static {
    try {
      isLzopIndexSupported =
        null != LzopCodec.class.getMethod("createIndexedOutputStream",
                                          OutputStream.class,
                                          DataOutputStream.class);
    } catch (Exception e) {
      // older version of hadoop-lzo.
    }
  }

  /**
   * Helper method to create lzo output file needed to create RecordWriter
   */
  protected DataOutputStream getOutputStream(TaskAttemptContext job)
                  throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    LzopCodec codec = new LzopCodec();
    codec.setConf(conf);

    Path file = getDefaultWorkFile(job, codec.getDefaultExtension());
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);

    FSDataOutputStream indexOut = null;
    if (conf.getBoolean("elephantbird.lzo.output.index", false)) {
      if ( isLzopIndexSupported ) {
        Path indexPath = file.suffix(LzoIndex.LZO_INDEX_SUFFIX);
        indexOut = fs.create(indexPath, false);
      } else {
        LOG.warn("elephantbird.lzo.output.index is enabled, but LzopCodec "
            + "does not have createIndexedOutputStream method. "
            + "Please upgrade hadoop-lzo.");
      }
    }

    OutputStream out = indexOut == null?
        codec.createOutputStream(fileOut) :
        codec.createIndexedOutputStream(fileOut, indexOut);

    return new DataOutputStream(out);
  }
}
