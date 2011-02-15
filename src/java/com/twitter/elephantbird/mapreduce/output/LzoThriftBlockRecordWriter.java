package com.twitter.elephantbird.mapreduce.output;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

public class LzoThriftBlockRecordWriter <T extends TBase<?, ?>, W extends ThriftWritable<T>>
extends LzoBinaryBlockRecordWriter<T, W> {

  public LzoThriftBlockRecordWriter(BinaryBlockWriter<T> writer) {
    super(writer);
  }

}
