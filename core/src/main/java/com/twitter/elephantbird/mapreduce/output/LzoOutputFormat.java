package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.LzoUtils;

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
   * Helper method to create lzo output file needed to create RecordWriter
   */
  protected DataOutputStream getOutputStream(TaskAttemptContext job)
                  throws IOException, InterruptedException {

    return LzoUtils.getIndexedLzoOutputStream(
                      job.getConfiguration(),
                      getDefaultWorkFile(job, LzopCodec.DEFAULT_LZO_EXTENSION));
  }
}
