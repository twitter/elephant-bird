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
    super(writableClass, getWritableConverterClass(messageClass, writableClass,
        ProtobufWritableConverter.class), messageClass.getName(), data, expected, valueSchema);
  }

  @SuppressWarnings("unchecked")
  private static <M extends Message, W extends ProtobufWritable<M>> Class<ProtobufWritableConverter<M, W>> getWritableConverterClass(
      Class<M> protobufClass, Class<W> writableClass, Class<?> cls) {
    return (Class<ProtobufWritableConverter<M, W>>) cls;
  }
}
