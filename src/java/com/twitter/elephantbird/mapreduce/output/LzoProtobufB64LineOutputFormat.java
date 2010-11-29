package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based output formats.
 * Data is written as one base64 encoded serialized protocol buffer per line. It has two template
 * parameters, the protobuf and the writable for that protobuf.  This class cannot be instantiated
 * directly as an input format because Hadoop works via reflection, and Java type erasure makes it
 * impossible to instantiate a templatized class via reflection with the correct template parameter.
 * Instead, we codegen derived output format classes for any given protobuf which instantiate the
 * template parameter directly, as well as set the typeRef argument so that the template
 * parameter can be remembered.  See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public abstract class LzoProtobufB64LineOutputFormat<M extends Message, W extends ProtobufWritable<M>>
    extends LzoOutputFormat<M, W> {
  protected TypeRef<M> typeRef_;

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  public RecordWriter<NullWritable, W> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    return new LzoBinaryB64LineRecordWriter<M, W>(new ProtobufConverter<M>(typeRef_), getOutputStream(job));
  }
}
