package com.twitter.elephantbird.pig.util;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestThriftWritableConverter<M extends TBase<?, ?>> extends
    AbstractTestWritableConverter<ThriftWritable<M>, ThriftWritableConverter<M>> {
  public AbstractTestThriftWritableConverter(Class<M> thriftClass,
      Class<? extends ThriftWritable<M>> writableClass,
      Class<? extends ThriftWritableConverter<M>> writableConverterClass, ThriftWritable<M>[] data,
      String[] expected, String valueSchema) {
    super(writableClass, writableConverterClass, thriftClass.getName(), data, expected, valueSchema);
  }

  @SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>, W extends ThriftWritable<M>> Class<W> getWritableClass(
      Class<M> thriftClass, Class<?> writableClass) {
    return (Class<W>) writableClass;
  }

  @SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>, W extends ThriftWritable<M>, C extends ThriftWritableConverter<M>> Class<C> getWritableConverterClass(
      Class<M> thriftClass, Class<W> writableClass, Class<?> writableConverterClass) {
    return (Class<C>) writableConverterClass;
  }
}
