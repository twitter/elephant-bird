package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.util.TypeRef;

/**
 * Reads line from an lzo compressed text file, base64 decodes it, and then
 * deserializes that into the Thrift object.
 * Returns <position, thriftObject> pairs. <br><br>
 *
 * Do not use LzoThriftB64LineInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() instead.<p>
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */
public class LzoThriftB64LineInputFormat<M extends TBase<?, ?>> extends MultiInputFormat<M> {

  public LzoThriftB64LineInputFormat() {}

  public LzoThriftB64LineInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }

  /**
   * Returns {@link LzoThriftB64LineInputFormat} class for setting up a job.
   * Sets an internal configuration in jobConf so that Task instantiates
   * appropriate object for this generic class based on thriftClass
   *
   * @Deprecated Use {@link MultiInputFormat#setInputFormatClass(Class, org.apache.hadoop.mapreduce.Job)
   */
  public static <M extends TBase<?, ?>> Class<LzoThriftB64LineInputFormat>
     getInputFormatClass(Class<M> thriftClass, Configuration jobConf) {
    setClassConf(thriftClass, jobConf);
    return LzoThriftB64LineInputFormat.class;
  }
}
