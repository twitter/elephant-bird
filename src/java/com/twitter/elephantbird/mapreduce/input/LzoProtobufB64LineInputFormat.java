package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.conf.Configuration;

import com.google.protobuf.Message;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based input formats.
 * Data is expected to be one base64 encoded serialized protocol buffer per line.
 * <br><br>
 *
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 *
 * @Deprecated use {@link MultiInputFormat}
 */
public class LzoProtobufB64LineInputFormat<M extends Message> extends MultiInputFormat<M> {

  public LzoProtobufB64LineInputFormat() {
    this(null, null);
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef,
      ProtobufExtensionRegistry extensionRegistry) {
    super(typeRef, extensionRegistry);
  }

  /**
   * Returns {@link LzoProtobufB64LineInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   *
   * @Deprecated Use {@link MultiInputFormat#setInputFormatClass(Class, org.apache.hadoop.mapreduce.Job)
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    return LzoProtobufB64LineInputFormat.getInputFormatClass(protoClass, null, jobConf);
  }

  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineInputFormat>
    getInputFormatClass(Class<M> protoClass,
        Class<? extends ProtobufExtensionRegistry> extRegClass,
        Configuration jobConf) {
    setClassConf(protoClass, jobConf);
    if(extRegClass != null) {
      setExtensionRegistryClassConf(extRegClass, jobConf);
    }
    return LzoProtobufB64LineInputFormat.class;
  }

  public static<M extends Message> LzoProtobufB64LineInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufB64LineInputFormat<M>(typeRef);
  }

  public static <M extends Message> LzoProtobufB64LineInputFormat<M> newInstance(
      TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    return new LzoProtobufB64LineInputFormat<M>(typeRef, extensionRegistry);
  }
}
