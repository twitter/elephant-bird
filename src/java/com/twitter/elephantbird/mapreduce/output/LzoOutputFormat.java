package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.hadoop.compression.lzo.LzopCodec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Base class for Lzo outputformats.
 * provides an helper method to create lzo output stream.
 */
public abstract class LzoOutputFormat<K, V> extends FileOutputFormat<K, V> {

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

    /* uncomment after https://github.com/kevinweil/hadoop-lzo/pull/23
    FSDataOutputStream indexOut = null;
    if (conf.getBoolean("elephantbird.lzo.output.index", false)) {
      Path indexPath = new Path(file.getParent(), file.getName() + ".index");
      indexOut = fs.create(indexPath, false);
    }

    return new DataOutputStream(codec.createIndexedOutputStream(fileOut, indexOut));
    */

    return new DataOutputStream(codec.createOutputStream(fileOut));
  }
}
