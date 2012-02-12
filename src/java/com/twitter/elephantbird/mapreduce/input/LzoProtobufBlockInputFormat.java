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
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it.
 * <br> <br>
 *
 * Do not use LzoProtobufBlockInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() instead.<p>
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */

public class LzoProtobufBlockInputFormat<M extends Message> extends LzoInputFormat<LongWritable, ProtobufWritable<M>> {
  private TypeRef<M> typeRef_;
  private ExtensionRegistry extensionRegistry_;

  public LzoProtobufBlockInputFormat() {
    this(null, null);
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef,
      ExtensionRegistry extensionRegistry) {
    typeRef_ = typeRef;
    extensionRegistry_ = extensionRegistry;
  }


  /**
   * Returns {@link LzoProtobufBlockInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufBlockInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    return LzoProtobufBlockInputFormat.getInputFormatClass(protoClass, null, jobConf);
  }

  @SuppressWarnings("rawtypes")
  public static <M extends Message> Class<LzoProtobufBlockInputFormat>
    getInputFormatClass(Class<M> protoClass,
        Class<? extends ProtobufExtensionRegistry<M>> extRegClass,
        Configuration jobConf) {
    Protobufs.setClassConf(jobConf, LzoProtobufBlockInputFormat.class, protoClass);

    if(extRegClass != null) {
      Protobufs.setExtensionRegistryClassConf(jobConf,
          LzoProtobufBlockInputFormat.class,
          extRegClass);
    }
    return LzoProtobufBlockInputFormat.class;
  }

  public static<M extends Message> LzoProtobufBlockInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockInputFormat<M>(typeRef);
  }

  public static <M extends Message> LzoProtobufBlockInputFormat<M> newInstance(
      TypeRef<M> typeRef, ExtensionRegistry extensionRegistry) {
    return new LzoProtobufBlockInputFormat<M>(typeRef, extensionRegistry);
  }

  @Override
  public RecordReader<LongWritable, ProtobufWritable<M>> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(taskAttempt.getConfiguration(), LzoProtobufBlockInputFormat.class);
    }

    if(extensionRegistry_ == null) {
      Class<? extends ProtobufExtensionRegistry<M>> extRegClass = Protobufs.getExtensionRegistryClassConf(
          taskAttempt.getConfiguration(), LzoProtobufBlockInputFormat.class);
      if(extRegClass != null) {
        extensionRegistry_ = Protobufs.safeNewInstance(extRegClass).getRealExtensionRegistry();
      }
    }

    return new LzoProtobufBlockRecordReader<M>(typeRef_, extensionRegistry_);
  }
}
