package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.output.LzoThriftB64LineOutputFormat;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Serializes Pig Tuples into Base-64 encoded, line-delimited Thrift objects.
 * The fields in the pig tuple must correspond exactly to the fields in
 * the Thrift object, as no name-matching is performed (names of the tuple
 * fields are not currently accessible to a StoreFunc. It will be in 0.7,
 * so something more flexible will be possible)
 */
public class LzoThriftB64LinePigStorage<T extends TBase<?, ?>> extends BaseStoreFunc {

  private TypeRef<T> typeRef;
  private ThriftWritable<T> writable;
  private PigToThrift<T> pigToThrift;

  public LzoThriftB64LinePigStorage(String thriftClassName) {
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    writable = ThriftWritable.newInstance(typeRef.getRawClass());
    pigToThrift = PigToThrift.newInstance(typeRef);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple f) throws IOException {
    if (f == null) return;
    try {
      writable.set(pigToThrift.getThriftObject(f));
      writer.write(null, writable);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputFormat<T, ThriftWritable<T>> getOutputFormat() throws IOException {
    return new LzoThriftB64LineOutputFormat<T>(typeRef);
  }
}
