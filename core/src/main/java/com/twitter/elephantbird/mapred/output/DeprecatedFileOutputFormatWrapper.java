package com.twitter.elephantbird.mapred.output;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * The wrapper enables an {@link FileOutputFormat} written for new
 * <code>mapreduce<code> interface to be used in contexts where
 * a {@link org.apache.hadoop.mapred.FileOutputFormat} old <code>mapred</code>
 * interface is required. </p>
 *
 * Note that this does not have a no args constructed, so it cannot currently
 * be used as an output format. Instead, it must be extended, such as in
 * {@link DeprecatedLzoTextOutputFormat}.
 *
 * @see DeprecatedOutputFormatWrapper
 *
 * @author Jonathan Coveney
 */
public class DeprecatedFileOutputFormatWrapper<K, V>
               extends org.apache.hadoop.mapred.FileOutputFormat<K, V> {

  private DeprecatedOutputFormatWrapper<K, V> wrapped;

  public DeprecatedFileOutputFormatWrapper(FileOutputFormat<K, V> wrapped) {
    this.wrapped = new DeprecatedOutputFormatWrapper<K, V>(wrapped);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    wrapped.checkOutputSpecs(ignored, job);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    return wrapped.getRecordWriter(ignored, job, name, progress);
  }
}
