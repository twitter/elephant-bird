package com.twitter.elephantbird.pig.util;

import com.google.protobuf.Message;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

/**
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestProtobufWritableConverter<M extends Message, W extends ProtobufWritable<M>>
    extends AbstractTestWritableConverter<W, ProtobufWritableConverter<M, W>> {
  public AbstractTestProtobufWritableConverter(Class<M> messageClass, Class<W> writableClass,
      W[] data, String[] expected, String valueSchema) {
    super(getWritableConverterClass(messageClass, writableClass, ProtobufWritableConverter.class),
        "-ca " + messageClass.getName(), writableClass, data, expected, valueSchema);
  }

  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<ProtobufWritable<M>> getWritableClass(
      Class<M> protobufClass, Class<?> cls) {
    return (Class<ProtobufWritable<M>>) cls;
  }

  @SuppressWarnings("unchecked")
  public static <M extends Message, W extends ProtobufWritable<M>> Class<ProtobufWritableConverter<M, W>> getWritableConverterClass(
      Class<M> protobufClass, Class<W> writableClass, Class<?> cls) {
    return (Class<ProtobufWritableConverter<M, W>>) cls;
  }
}
