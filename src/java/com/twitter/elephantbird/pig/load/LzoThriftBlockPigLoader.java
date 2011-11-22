package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

public class LzoThriftBlockPigLoader<M extends TBase<?, ?>> extends LzoThriftB64LinePigLoader<M> {

  public LzoThriftBlockPigLoader(String thriftClassName) {
    super(thriftClassName);
  }

  @Override
  public InputFormat<LongWritable, ThriftWritable<M>> getInputFormat() throws IOException {
    return new LzoThriftBlockInputFormat<M>(typeRef_);
  }
}
