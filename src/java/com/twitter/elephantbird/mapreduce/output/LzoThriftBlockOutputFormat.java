package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;

/**
 * Data is written as one base64 encoded serialized thrift per line. <br><br>
 *
 * Do not use LzoThriftB64LineOutputFormat.class directly for setting
 * OutputFormat class for a job. Use getOutputFormatClass() instead.
 */
public class LzoThriftBlockOutputFormat<M extends TBase<?, ?>>
    extends LzoOutputFormat<M, ThriftWritable<M>> {

  protected TypeRef<M> typeRef_;

  public LzoThriftBlockOutputFormat() {}

  public LzoThriftBlockOutputFormat(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  @SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>> Class<LzoThriftBlockOutputFormat>
     getOutputFormatClass(Class<M> thriftClass, Configuration jobConf) {

    ThriftUtils.setClassConf(jobConf, LzoThriftBlockOutputFormat.class, thriftClass);
    return LzoThriftBlockOutputFormat.class;
  }

  public RecordWriter<M, ThriftWritable<M>> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = ThriftUtils.getTypeRef(job.getConfiguration(), LzoThriftBlockOutputFormat.class);
    }
    return new LzoBinaryBlockRecordWriter<M, ThriftWritable<M>>(
        new ThriftBlockWriter<M>(getOutputStream(job), typeRef_.getRawClass()));
  }
}
