package com.twitter.elephantbird.pig.util;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestThriftWritableConverter<M extends TBase<?, ?>, W extends ThriftWritable<M>>
    extends AbstractTestWritableConverter<W, ThriftWritableConverter<M, W>> {
  public AbstractTestThriftWritableConverter(Class<M> thriftClass, Class<W> writableClass,
      W[] data, String[] expected, String valueSchema) {
    super(getWritableConverterClass(thriftClass, writableClass, ThriftWritableConverter.class),
        "-ca " + thriftClass.getName(), writableClass, data, expected, valueSchema);
  }

  @SuppressWarnings("unchecked")
  private static <M extends TBase<?, ?>, W extends ThriftWritable<M>> Class<ThriftWritableConverter<M, W>> getWritableConverterClass(
      Class<M> thriftClass, Class<W> writableClass, Class<?> cls) {
    return (Class<ThriftWritableConverter<M, W>>) cls;
  }
}
