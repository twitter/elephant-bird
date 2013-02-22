package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.output.RCFileThriftOutputFormat;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * StoreFunc for storing Thrift objects in RCFiles. <p>
 *
 * @see RCFileThriftOutputFormat
 */
public class RCFileThriftPigStorage extends BaseStoreFunc {
  // add stats?

  private final TypeRef<? extends TBase<?, ?>>  typeRef;
  private final ThriftWritable<TBase<?, ?>>     writable;
  private final PigToThrift<TBase<?, ?>>        pigToThrift;

  @SuppressWarnings("unchecked")
  public RCFileThriftPigStorage(String thriftClassName) {
    typeRef =  ThriftUtils.getTypeRef(thriftClassName);
    pigToThrift = (PigToThrift<TBase<?, ?>>) PigToThrift.newInstance(typeRef);
    writable = new ThriftWritable(typeRef);
  }

  @Override @SuppressWarnings("unchecked")
  public OutputFormat getOutputFormat() throws IOException {
    return new RCFileThriftOutputFormat(typeRef);
  }

  public void putNext(Tuple t) throws IOException {
    TBase<?, ?> tObj = pigToThrift.getThriftObject(t);
    writable.set(tObj);
    writeRecord(null, writable);
  }

}
