package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Class for all blocked protocol buffer based output formats.  See
 * the ProtobufBlockWriter class for the on-disk format. <br><br>
 *
 * Do not use LzoProtobufBlockOutputFormat.class directly for setting
 * OutputFormat class for a job. Use getOutputFormatClass() or getInstance() instead.
 */

public class LzoProtobufBlockOutputFormat<M extends Message> extends LzoOutputFormat<M, ProtobufWritable<M>> {

  protected TypeRef<M> typeRef_;
  protected ExtensionRegistry extensionRegistry_;

  public LzoProtobufBlockOutputFormat() {
    this(null, null);
  }

  public LzoProtobufBlockOutputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufBlockOutputFormat(TypeRef<M> typeRef,
      ExtensionRegistry extensionRegistry) {
    typeRef_ = typeRef;
    extensionRegistry_ = extensionRegistry;
  }


  /**
   * Returns {@link LzoProtobufB64LineOutputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object for this generic class based on protoClass
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufBlockOutputFormat>
  getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {
    return LzoProtobufBlockOutputFormat.getOutputFormatClass(protoClass, null, jobConf);
  }

  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufBlockOutputFormat>
    getOutputFormatClass(Class<M> protoClass,
        Class<? extends ProtobufExtensionRegistry<M>> extRegClass,
        Configuration jobConf) {
    Protobufs.setClassConf(jobConf, LzoProtobufBlockOutputFormat.class, protoClass);

    if(extRegClass != null) {
      Protobufs.setExtensionRegistryClassConf(jobConf,
          LzoProtobufBlockOutputFormat.class,
          extRegClass);
    }
    return LzoProtobufBlockOutputFormat.class;
  }

  public static<M extends Message> LzoProtobufBlockOutputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockOutputFormat<M>(typeRef);
  }

  public static <M extends Message> LzoProtobufBlockOutputFormat<M> newInstance(
      TypeRef<M> typeRef, ExtensionRegistry extensionRegistry) {
    return new LzoProtobufBlockOutputFormat<M>(typeRef, extensionRegistry);
  }

  @Override
  public RecordWriter<M, ProtobufWritable<M>> getRecordWriter(TaskAttemptContext job)
  throws IOException, InterruptedException {
    if (typeRef_ == null) { // i.e. if not set by a subclass
      typeRef_ = Protobufs.getTypeRef(job.getConfiguration(), LzoProtobufBlockOutputFormat.class);
    }

    if(extensionRegistry_ == null) {
      Class<? extends ProtobufExtensionRegistry<M>> factoryClass = Protobufs.getExtensionRegistryClassConf(
          job.getConfiguration(), LzoProtobufBlockOutputFormat.class);
      if(factoryClass != null) {
        extensionRegistry_ = Protobufs.safeNewInstance(factoryClass).getRealExtensionRegistry();
      }
    }

    return new LzoBinaryBlockRecordWriter<M, ProtobufWritable<M>>(
        new ProtobufBlockWriter<M>(getOutputStream(job), typeRef_.getRawClass(),
            extensionRegistry_));
  }
}
