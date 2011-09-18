package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * Supports conversion between Pig {@link Tuple} and {@link ThriftWritable} types. This is a
 * specialization of {@link ThriftWritableConverter} which uses
 * {@link ThriftToPig#getLazyTuple(TBase)} to generate Tuples from ThriftWritable instances.
 *
 * @author Andy Schlaikjer
 * @see ThriftWritableConverter
 */
public class LazyThriftWritableConverter<M extends TBase<?, ?>> extends ThriftWritableConverter<M> {
  public LazyThriftWritableConverter(String thriftClassName) {
    super(thriftClassName);
  }

  @Override
  protected Tuple toTuple(ThriftWritable<M> writable, ResourceFieldSchema schema)
      throws IOException {
    return thriftToPig.getLazyTuple(writable.get());
  }
}
