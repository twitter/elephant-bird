package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
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
  private ExtensionRegistry extensionRegistry_;

  public LzoProtobufB64LineInputFormat() {
    this(null, null);
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef,
      ExtensionRegistry extensionRegistry) {
    super();
    this.typeRef_ = typeRef;
    this.extensionRegistry_ = extensionRegistry;
  }

  /**
   * Returns {@link LzoProtobufB64LineInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    return LzoProtobufB64LineInputFormat.getInputFormatClass(protoClass, null, jobConf);
  }

  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineInputFormat>
    getInputFormatClass(Class<M> protoClass,
        Class<? extends ProtobufExtensionRegistry<M>> extRegClass,
        Configuration jobConf) {
    Protobufs.setClassConf(jobConf, LzoProtobufB64LineInputFormat.class, protoClass);

    if(extRegClass != null) {
      Protobufs.setExtensionRegistryClassConf(jobConf,
          LzoProtobufB64LineInputFormat.class,
          extRegClass);
    }

    return LzoProtobufB64LineInputFormat.class;
  }

  public static<M extends Message> LzoProtobufB64LineInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufB64LineInputFormat<M>(typeRef);
  }

  public static <M extends Message> LzoProtobufB64LineInputFormat<M> newInstance(
      TypeRef<M> typeRef, ExtensionRegistry extensionRegistry) {
    return new LzoProtobufB64LineInputFormat<M>(typeRef, extensionRegistry);
  }

  @Override
  public RecordReader<LongWritable, ProtobufWritable<M>> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(taskAttempt.getConfiguration(), LzoProtobufB64LineInputFormat.class);
    }

    if(extensionRegistry_ == null) {
      Class<? extends ProtobufExtensionRegistry<M>> extRegClass = Protobufs.getExtensionRegistryClassConf(
          taskAttempt.getConfiguration(), LzoProtobufB64LineInputFormat.class);
      if(extRegClass != null) {
        extensionRegistry_ = Protobufs.safeNewInstance(extRegClass).getRealExtensionRegistry();
      }
    }
    return new LzoProtobufB64LineRecordReader<M>(typeRef_, extensionRegistry_);
  }
}
