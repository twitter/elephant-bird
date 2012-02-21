package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.conf.Configuration;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it.
 * <br> <br>
 *
 * Do not use LzoProtobufBlockInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() instead.<p>
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */

public class LzoProtobufBlockInputFormat<M extends Message> extends MultiInputFormat<M> {

  public LzoProtobufBlockInputFormat() {
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }

  /**
   * Returns {@link LzoProtobufBlockInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   *
   * @Deprecated Use {@link MultiInputFormat#setInputFormatClass(Class, org.apache.hadoop.mapreduce.Job)
   */
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<LzoProtobufBlockInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    setClassConf(protoClass, jobConf);
    return LzoProtobufBlockInputFormat.class;
  }

  public static<M extends Message> LzoProtobufBlockInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockInputFormat<M>(typeRef);
  }
}
