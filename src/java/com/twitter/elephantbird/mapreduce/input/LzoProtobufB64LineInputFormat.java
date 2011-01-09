package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based input formats.
 * Data is expected to be one base64 encoded serialized protocol buffer per line.
 * <br><br>
 *
 * Do not use LzoProtobufB64LineInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() or newInstance(typeRef) instead.
 */

public class LzoProtobufB64LineInputFormat<M extends Message> extends LzoInputFormat<LongWritable, ProtobufWritable<M>> {

  private TypeRef<M> typeRef_;

  public LzoProtobufB64LineInputFormat() {
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef) {
    super();
    this.typeRef_ = typeRef;
  }

  // should remove this.
  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  /**
   * Returns {@link LzoProtobufB64LineInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    Protobufs.setClassConf(jobConf, LzoProtobufB64LineInputFormat.class, protoClass);
    return LzoProtobufB64LineInputFormat.class;
  }

  public static<M extends Message> LzoProtobufB64LineInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufB64LineInputFormat<M>(typeRef);
  }

  @Override
  public RecordReader<LongWritable, ProtobufWritable<M>> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(taskAttempt.getConfiguration(), LzoProtobufB64LineInputFormat.class);
    }
    return new LzoProtobufB64LineRecordReader<M>(typeRef_);
  }
}
