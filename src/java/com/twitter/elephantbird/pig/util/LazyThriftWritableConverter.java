package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * Supports conversion between Pig Tuple and Thrift types.
 *
 * @author Andy Schlaikjer
 */
public class LazyThriftWritableConverter<M extends TBase<?, ?>, W extends ThriftWritable<M>>
    extends ThriftWritableConverter<M, W> {
  public LazyThriftWritableConverter(String thriftClassName) {
    super(thriftClassName);
  }

  @Override
  protected Tuple toTuple(W writable, ResourceFieldSchema schema) throws IOException {
    return thriftToPig.getLazyTuple(writable.get());
  }
}
