package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based input formats.
 * Data is expected to be one base64 encoded serialized protocol buffer per line. It has two template
 * parameters, the protobuf and the writable for that protobuf.  This class cannot be instantiated
 * directly as an input format because Hadoop works via reflection, and Java type erasure makes it
 * impossible to instantiate a templatized class via reflection with the correct template parameter.
 * Instead, we codegen derived input format classes for any given protobuf which instantiate the
 * template parameter directly, as well as set the typeRef argument so that the template
 * parameter can be remembered.  See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public abstract class LzoProtobufB64LineInputFormat<M extends Message, W extends ProtobufWritable<M>> extends LzoInputFormat<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LineInputFormat.class);

  private TypeRef<M> typeRef_;
  private W protobufWritable_;

  public LzoProtobufB64LineInputFormat() {
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

    return new LzoProtobufB64LineRecordReader<M, W>(typeRef_, protobufWritable_);
  }
}
