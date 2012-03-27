package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

import org.apache.thrift.TBase;

/**
 * A writer for LZO-encoded blocks of Thrift objects.
 *
 */
public class DeprecatedLzoThriftBlockRecordWriter<M extends TBase<?,?>>
    extends DeprecatedLzoBinaryBlockRecordWriter<M, ThriftWritable<M>> {

  public DeprecatedLzoThriftBlockRecordWriter(ThriftBlockWriter<M> writer) {
    super(writer);
  }
}
