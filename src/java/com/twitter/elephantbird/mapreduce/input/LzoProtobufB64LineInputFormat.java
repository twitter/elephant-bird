package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based input formats.
 * Data is expected to be one base64 encoded serialized protocol buffer per line.
 * <br><br>
 *
 * Do not use LzoProtobufB64LineInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() or newInstance(typeRef) instead.
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */

public class LzoProtobufB64LineInputFormat<M extends Message> extends LzoInputFormat<LongWritable, ProtobufWritable<M>> {

  private TypeRef<M> typeRef_;
  private List<GeneratedExtension<M, ?>> protoExtensions_;

  public LzoProtobufB64LineInputFormat() {
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef,
      List<GeneratedExtension<M, ?>> protoExtensions) {
    super();
    this.typeRef_ = typeRef;
    this.protoExtensions_ = protoExtensions;
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

  public static <M extends Message> Class<LzoProtobufB64LineInputFormat>
    getInputFormatClass(Class<M> protoClass, List<Class<?>> protoExtensionClasses,
        Configuration jobConf) {

    //TODO:

    return null;
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

    if(protoExtensions_ == null) {

    }
    return new LzoProtobufB64LineRecordReader<M>(typeRef_, protoExtensions_);
  }
}
