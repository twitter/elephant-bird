package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.input.LzoRecordReader;
import com.twitter.elephantbird.mapreduce.input.LzoThriftB64LineInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ThriftToPig;

import com.twitter.elephantbird.util.TypeRef;

public class LzoThriftB64LinePigLoader<M extends TBase<?, ?>> extends LzoBaseLoadFunc {

  private final TypeRef<M> typeRef_;
  private final ThriftToPig<M> thriftToPig_;

  public LzoThriftB64LinePigLoader(String thriftClassName) {
    typeRef_ = PigUtil.getThriftTypeRef(thriftClassName);
    thriftToPig_ =  ThriftToPig.newInstance(typeRef_);
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   *<p>
   * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
   * for more information on error handling.
   */
  @Override
  public Tuple getNext() throws IOException {
    M value = getNextBinaryValue(typeRef_);

    return value != null ?
      thriftToPig_.getLazyTuple(value) : null;
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    return new ResourceSchema(ThriftToPig.toSchema(typeRef_.getRawClass()));
  }

  @Override
  public InputFormat<LongWritable, ThriftWritable<M>> getInputFormat() throws IOException {
    return new LzoThriftB64LineInputFormat<M>(typeRef_);
  }
}
