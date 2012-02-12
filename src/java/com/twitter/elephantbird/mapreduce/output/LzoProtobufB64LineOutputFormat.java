package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
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
  protected ExtensionRegistry extensionRegistry_;

  public LzoProtobufB64LineOutputFormat() {
    this(null, null);
  }

  public LzoProtobufB64LineOutputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufB64LineOutputFormat(TypeRef<M> typeRef,
      ExtensionRegistry extensionRegistry) {
    typeRef_ = typeRef;
    extensionRegistry_ = extensionRegistry;
  }

  /**
   * Returns {@link LzoProtobufBlockOutputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object for this generic class based on protoClass
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineOutputFormat>
  getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {
    return LzoProtobufB64LineOutputFormat.getOutputFormatClass(protoClass, null, jobConf);
  }

  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufB64LineOutputFormat>
    getOutputFormatClass(Class<M> protoClass,
        Class<? extends ProtobufExtensionRegistry<M>> extRegClass,
        Configuration jobConf) {
    Protobufs.setClassConf(jobConf, LzoProtobufB64LineOutputFormat.class, protoClass);

    if(extRegClass != null) {
      Protobufs.setExtensionRegistryClassConf(jobConf,
          LzoProtobufB64LineOutputFormat.class,
          extRegClass);
    }

    return LzoProtobufB64LineOutputFormat.class;
  }

  @Override
  public RecordWriter<M, ProtobufWritable<M>> getRecordWriter(TaskAttemptContext job)
  throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(job.getConfiguration(), LzoProtobufB64LineOutputFormat.class);
    }

    if(extensionRegistry_ == null) {
      Class<? extends ProtobufExtensionRegistry<M>> extRegClass = Protobufs.getExtensionRegistryClassConf(
          job.getConfiguration(), LzoProtobufB64LineOutputFormat.class);
      if(extRegClass != null) {
        extensionRegistry_ = Protobufs.safeNewInstance(extRegClass).getRealExtensionRegistry();
      }
    }

    return new LzoBinaryB64LineRecordWriter<M, ProtobufWritable<M>>(
        ProtobufConverter.newInstance(typeRef_, extensionRegistry_), getOutputStream(job));
  }
}
