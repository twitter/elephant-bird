package com.twitter.elephantbird.pig.util;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestThriftWritableConverter<M extends TBase<?, ?>> extends
    AbstractTestWritableConverter<ThriftWritable<M>, ThriftWritableConverter<M>> {
  public AbstractTestThriftWritableConverter(Class<M> thriftClass, ThriftWritable<M>[] data,
      String[] expected, String valueSchema) {
    super(getWritableClass(thriftClass, ThriftWritable.class), getWritableConverterClass(
        thriftClass, ThriftWritableConverter.class), thriftClass.getName(), data, expected,
        valueSchema);
  }

  @SuppressWarnings("unchecked")
  private static <M extends TBase<?, ?>> Class<ThriftWritable<M>> getWritableClass(
      Class<M> thriftClass, Class<?> cls) {
    return (Class<ThriftWritable<M>>) cls;
  }

  @SuppressWarnings("unchecked")
  private static <M extends TBase<?, ?>> Class<ThriftWritableConverter<M>> getWritableConverterClass(
      Class<M> thriftClass, Class<?> cls) {
    return (Class<ThriftWritableConverter<M>>) cls;
  }
}
