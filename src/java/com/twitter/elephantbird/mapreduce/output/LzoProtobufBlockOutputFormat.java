package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
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

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  public LzoProtobufBlockOutputFormat() {}

  public LzoProtobufBlockOutputFormat(TypeRef<M> typeRef) {
    this.typeRef_ = typeRef;
  }

  /**
   * Returns {@link LzoProtobufB64LineOutputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object for this generic class based on protoClass
   */
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<LzoProtobufBlockOutputFormat>
  getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {

    Protobufs.setClassConf(jobConf, LzoProtobufBlockOutputFormat.class, protoClass);
    return LzoProtobufBlockOutputFormat.class;
  }

  public static<M extends Message> LzoProtobufBlockOutputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockOutputFormat<M>(typeRef);
  }

  @Override
  public RecordWriter<M, ProtobufWritable<M>> getRecordWriter(TaskAttemptContext job)
  throws IOException, InterruptedException {
    if (typeRef_ == null) { // i.e. if not set by a subclass
      typeRef_ = Protobufs.getTypeRef(job.getConfiguration(), LzoProtobufBlockOutputFormat.class);
    }

    return new LzoBinaryBlockRecordWriter<M, ProtobufWritable<M>>(
        new ProtobufBlockWriter<M>(getOutputStream(job), typeRef_.getRawClass()));
  }
}
