package com.twitter.elephantbird.mapred.output;

import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.util.HadoopUtils;

/**
 * The wrapper enables an {@link OutputFormat} written for new
 * <code>mapreduce<code> interface to be used in contexts where
 * a {@link org.apache.hadoop.mapred.OutputFormat} old <code>mapred</code>
 * interface is required. </p>
 *
 * Usage: <pre>
 *    jobConf.setInputFormat(anInputFormat.class);
 *    //set OutputFormat class using a mapreduce OutputFormat
 *    DeprecatedOutputFormatWrapper.setOutputFormat(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class, jobConf);
 * <pre>
 *
 * @see DeprecatedInputFormatWrapper
 *
 * @author Raghu Angadi
 */
@SuppressWarnings("deprecation")
public class DeprecatedOutputFormatWrapper<K, V>
                      implements org.apache.hadoop.mapred.OutputFormat<K, V> {

  protected OutputFormat<K, V> realOutputFormat;

  private static final String CLASS_CONF_KEY = "elephantbird.class.for.DeprecatedOutputFormatWrapper";

  public DeprecatedOutputFormatWrapper() {}

  public DeprecatedOutputFormatWrapper(OutputFormat<K, V> mapreduceOutputFormat) {
    realOutputFormat = mapreduceOutputFormat;
  }

  /**
   * Sets jobs output format to {@link DeprecatedOutputFormatWrapper} and stores
   * supplied real {@link OutputFormat} class name in job configuration.
   * This configuration is read on the remote tasks to instantiate actual
   * OutputFormat correctly.
   */
  public static void setOutputFormat(Class<?> realOutputFormatClass, JobConf jobConf) {
    jobConf.setOutputFormat(DeprecatedOutputFormatWrapper.class);
    HadoopUtils.setClassConf(jobConf, CLASS_CONF_KEY, realOutputFormatClass);
  }

  @SuppressWarnings("unchecked")
  private void initOutputFormat(JobConf conf) {
    if (realOutputFormat == null) {
      realOutputFormat = ReflectionUtils.newInstance(
                           conf.getClass(CLASS_CONF_KEY, null, OutputFormat.class),
                           conf);
    }
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    initOutputFormat(job);
    try {
      realOutputFormat.checkOutputSpecs(HadoopCompat.newJobContext(job, null));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    initOutputFormat(job);
    return new RecordWriterWrapper<K, V>(realOutputFormat, job, name, progress);
  }

  private static class RecordWriterWrapper<K, V> implements RecordWriter<K, V> {

    private org.apache.hadoop.mapreduce.RecordWriter<K, V> realWriter;
    private TaskAttemptContext taskContext;

    @SuppressWarnings("unchecked")
    RecordWriterWrapper(OutputFormat<K, V> realOutputFormat,
                        JobConf jobConf, String name, Progressable progress)
                        throws IOException {
      try {
        // create a MapContext to provide access to the reporter (for counters)
        taskContext = HadoopCompat.newMapContext(
            jobConf, TaskAttemptID.forName(jobConf.get("mapred.task.id")),
            null, null, null, (StatusReporter) progress, null);

        realWriter = realOutputFormat.getRecordWriter(taskContext);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      try {
        // create a context just to pass reporter
        realWriter.close(taskContext);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void write(K key, V value) throws IOException {
      try {
        realWriter.write(key, value);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
}
