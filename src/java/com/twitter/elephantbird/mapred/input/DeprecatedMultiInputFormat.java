package com.twitter.elephantbird.mapred.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * mapred version of {@link MultiInputFormat}
 */
public class DeprecatedMultiInputFormat<M>
          extends DeprecatedInputFormatWrapper<LongWritable, BinaryWritable<M>>{

  public DeprecatedMultiInputFormat() {
    super(new MultiInputFormat<M>());
  }

  public DeprecatedMultiInputFormat(TypeRef<M> typeRef) {
    super(new MultiInputFormat<M>(typeRef));
  }

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  public static void setClassConf(Class<?> clazz, Configuration conf) {
    MultiInputFormat.setClassConf(clazz, conf);
  }
}
