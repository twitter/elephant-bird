package com.twitter.elephantbird.mapred.output;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.LzoUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Base class for Lzo outputformats.
 *
 * @author Yifan Shi
 */
public abstract class DeprecatedLzoOutputFormat <K, V>
    extends FileOutputFormat<K, V> {

  /**
   * Helper method to create lzo output file needed to create RecordWriter
   */
  protected DataOutputStream getOutputStream(JobConf job)
                  throws IOException {
    LzopCodec codec = new LzopCodec();
    codec.setConf(job);

    Path file = getPathForCustomFile(job,  "part");
    file = file.suffix(codec.getDefaultExtension());

    return LzoUtils.getIndexedLzoOutputStream(job, file);
  }
}
