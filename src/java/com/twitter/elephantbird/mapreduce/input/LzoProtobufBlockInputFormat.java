package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it. It has two template
 * parameters, the protobuf and the writable for that protobuf.  This class cannot be instantiated
 * directly as an input format because Hadoop works via reflection, and Java type erasure makes it
 * impossible to instantiate a templatized class via reflection with the correct template parameter.
 * Instead, we codegen derived input format classes for any given protobuf which instantiate the
 * template parameter directly, as well as set the typeRef argument so that the template
 * parameter can be remembered.  See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public abstract class LzoProtobufBlockInputFormat<M extends Message, W extends ProtobufWritable<M>> extends LzoInputFormat<LongWritable, W> {

  private TypeRef<M> typeRef_;
  private W protobufWritable_;

  public LzoProtobufBlockInputFormat() {
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  protected void setProtobufWritable(W protobufWritable) {
    protobufWritable_ = protobufWritable;
  }

  @Override
  public RecordReader<LongWritable, W> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {

    return new LzoProtobufBlockRecordReader<M, W>(typeRef_, protobufWritable_);
  }
}
