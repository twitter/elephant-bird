package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

/**
 * Reads Thrift objects written in blocks using LzoThriftBlockOutputFormat
 * <br><br>
 *
 * Do not use LzoThriftBlockInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() instead.
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */
public class LzoThriftBlockInputFormat<M extends TBase<?, ?>> extends MultiInputFormat<M> {

  public LzoThriftBlockInputFormat() {}

  public LzoThriftBlockInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }

  /**
   * Returns {@link LzoThriftBlockInputFormat} class for setting up a job.
   * Sets an internal configuration in jobConf so that Task instantiates
   * appropriate object for this generic class based on thriftClass
   *
   * @Deprecated Use {@link MultiInputFormat#setInputFormatClass(Class, org.apache.hadoop.mapreduce.Job)
   */
  @SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>> Class<LzoThriftBlockInputFormat>
     getInputFormatClass(Class<M> thriftClass, Configuration jobConf) {
    setClassConf(thriftClass, jobConf);
    return LzoThriftBlockInputFormat.class;
  }
}
