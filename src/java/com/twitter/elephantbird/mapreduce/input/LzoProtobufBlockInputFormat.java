package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.conf.Configuration;

import com.google.protobuf.Message;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it.
 * <br> <br>
 *
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 *
 * @Deprecated use {@link MultiInputFormat}
 */
public class LzoProtobufBlockInputFormat<M extends Message> extends MultiInputFormat<M> {

  public LzoProtobufBlockInputFormat() {
    this(null, null);
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef,
      ProtobufExtensionRegistry extensionRegistry) {
    super(typeRef, extensionRegistry);
  }

  /**
   * Returns {@link LzoProtobufBlockInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   *
   * @Deprecated Use {@link MultiInputFormat#setInputFormatClass(Class, org.apache.hadoop.mapreduce.Job)
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufBlockInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    return LzoProtobufBlockInputFormat.getInputFormatClass(protoClass, null, jobConf);
  }

  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufBlockInputFormat>
    getInputFormatClass(Class<M> protoClass,
        Class<? extends ProtobufExtensionRegistry> extRegClass,
        Configuration jobConf) {
    setClassConf(protoClass, jobConf);

    if(extRegClass != null) {
      setExtensionRegistryClassConf(extRegClass, jobConf);
    }
    return LzoProtobufBlockInputFormat.class;
  }

  public static<M extends Message> LzoProtobufBlockInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockInputFormat<M>(typeRef);
  }

  public static <M extends Message> LzoProtobufBlockInputFormat<M> newInstance(
      TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    return new LzoProtobufBlockInputFormat<M>(typeRef, extensionRegistry);
  }
}
