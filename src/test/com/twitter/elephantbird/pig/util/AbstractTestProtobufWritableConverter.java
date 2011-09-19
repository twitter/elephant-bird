package com.twitter.elephantbird.pig.util;

import com.google.protobuf.Message;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

/**
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestProtobufWritableConverter<M extends Message> extends
    AbstractTestWritableConverter<ProtobufWritable<M>, ProtobufWritableConverter<M>> {
  public AbstractTestProtobufWritableConverter(Class<M> protobufClass, ProtobufWritable<M>[] data,
      String[] expected, String valueSchema) {
    super(castProtobufWritableClass(protobufClass, ProtobufWritable.class),
        castProtobufWritableConverterClass(protobufClass, ProtobufWritableConverter.class),
        protobufClass.getName(), data, expected, valueSchema);
  }

  @SuppressWarnings("unchecked")
  private static <M extends Message> Class<ProtobufWritable<M>> castProtobufWritableClass(
      Class<M> protobufClass, Class<?> cls) {
    return (Class<ProtobufWritable<M>>) cls;
  }

  @SuppressWarnings("unchecked")
  private static <M extends Message> Class<ProtobufWritableConverter<M>> castProtobufWritableConverterClass(
      Class<M> protobufClass, Class<?> cls) {
    return (Class<ProtobufWritableConverter<M>>) cls;
  }
}
