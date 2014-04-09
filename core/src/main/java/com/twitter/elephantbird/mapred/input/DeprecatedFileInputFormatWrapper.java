package com.twitter.elephantbird.mapred.input;

import java.io.IOException;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * A {@Link DeprecatedInputFormatWrapper} that looks like
 * a regular file input format
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("deprecation")
public class DeprecatedFileInputFormatWrapper<K, V>
  extends FileInputFormat<K, V> {

  protected DeprecatedInputFormatWrapper<K, V> wrapper;

  public DeprecatedFileInputFormatWrapper() {
    wrapper = new DeprecatedInputFormatWrapper<K, V>();
  }

  public DeprecatedFileInputFormatWrapper(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V> realInputFormat) {
    wrapper = new DeprecatedInputFormatWrapper<K, V>(realInputFormat);
  }

  /**
   * @see DeprecatedInputFormatWrapper.setInputFormat
   * @param realInputFormatClass
   * @param jobConf
   */
  public static void setInputFormat(Class<?> realInputFormatClass, JobConf jobConf) {
    DeprecatedInputFormatWrapper.setInputFormat(realInputFormatClass, jobConf);
  }

  /**
   * @see DeprecatedInputFormatWrapper.setInputFormat
   * @param realInputFormatClass
   * @param jobConf
   * @param valueCopyClass
   */
  public static void setInputFormat(Class<?> realInputFormatClass, JobConf jobConf,
      Class<? extends DeprecatedInputFormatValueCopier<?>> valueCopyClass) {
    DeprecatedInputFormatWrapper.setInputFormat(realInputFormatClass, jobConf, valueCopyClass);
  }

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return wrapper.getRecordReader(split, job, reporter);
  }


  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return wrapper.getSplits(job, numSplits);
  }

  public void setInputFormatInstance(InputFormat<K, V> inputFormat) {
    wrapper.setInputFormatInstance(inputFormat);
  }
}
