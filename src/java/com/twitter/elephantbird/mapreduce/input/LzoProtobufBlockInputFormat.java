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
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it.
 * <br> <br>
 *
 * Do not use LzoProtobufBlockInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() instead.
 */

public class LzoProtobufBlockInputFormat<M extends Message> extends LzoInputFormat<LongWritable, ProtobufWritable<M>> {

  private TypeRef<M> typeRef_;

  public LzoProtobufBlockInputFormat() {
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef) {
    super();
    this.typeRef_ = typeRef;
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  /**
   * Returns {@link LzoProtobufBlockInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<LzoProtobufBlockInputFormat>
     getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
    Protobufs.setClassConf(jobConf, LzoProtobufBlockInputFormat.class, protoClass);
    return LzoProtobufBlockInputFormat.class;
  }

  public static<M extends Message> LzoProtobufBlockInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockInputFormat<M>(typeRef);
  }

  @Override
  public RecordReader<LongWritable, ProtobufWritable<M>> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(taskAttempt.getConfiguration(), LzoProtobufBlockInputFormat.class);
    }
    return new LzoProtobufBlockRecordReader<M>(typeRef_);
  }
}
