package com.twitter.elephantbird.mapred.output;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Base class for Lzo outputformats.
 *
 * @author Yifan Shi
 */
public abstract class DeprecatedLzoOutputFormat <M, W>
    extends FileOutputFormat<NullWritable, W> {

  /**
   * Helper method to create lzo output file needed to create RecordWriter
   */
  protected DataOutputStream getOutputStream(JobConf job)
                  throws IOException {
    LzopCodec codec = new LzopCodec();
    codec.setConf(job);

    Path file = getPathForCustomFile(job,  "part");
    file = file.suffix(codec.getDefaultExtension());
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, false);
    return new DataOutputStream(codec.createOutputStream(fileOut));
  }
}
