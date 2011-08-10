package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the class for all base64 encoded, line-oriented protocol buffer based output formats.
 * Data is written as one base64 encoded serialized protocol buffer per line.<br><br>
 *
 * Do not use LzoProtobufB64LineOutputFormat.class directly for setting
 * OutputFormat class for a job. Use getOutputFormatClass() or getInstance() instead.
 */

public class LzoProtobufB64LineOutputFormat<M extends Message> extends LzoOutputFormat<M, ProtobufWritable<M>> {
  protected TypeRef<M> typeRef_;

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  public LzoProtobufB64LineOutputFormat() {}

  public LzoProtobufB64LineOutputFormat(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  /**
   * Returns {@link LzoProtobufBlockOutputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object for this generic class based on protoClass
   */
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<LzoProtobufB64LineOutputFormat>
  getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {

    Protobufs.setClassConf(jobConf, LzoProtobufB64LineOutputFormat.class, protoClass);
    return LzoProtobufB64LineOutputFormat.class;
  }

  @Override
  public RecordWriter<M, ProtobufWritable<M>> getRecordWriter(TaskAttemptContext job)
  throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(job.getConfiguration(), LzoProtobufB64LineOutputFormat.class);
    }
    return new LzoBinaryB64LineRecordWriter<M, ProtobufWritable<M>>(ProtobufConverter.newInstance(typeRef_), getOutputStream(job));
  }
}
