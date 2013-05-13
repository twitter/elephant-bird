package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;

/**
 * Data is written as one base64 encoded serialized thrift per line. <br><br>
 *
 * Do not forget to set Thrift class using setClassConf().
 */
public class LzoThriftB64LineOutputFormat<M extends TBase<?, ?>>
    extends LzoOutputFormat<M, ThriftWritable<M>> {

  protected TypeRef<M> typeRef_;

  public LzoThriftB64LineOutputFormat() {}

  public LzoThriftB64LineOutputFormat(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  /**
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object for this generic class based on thriftClass
   */
  public static <M extends TBase<?, ?>>
  void setClassConf(Class<M> thriftClass, Configuration jobConf) {
    ThriftUtils.setClassConf(jobConf,
                             LzoThriftB64LineOutputFormat.class,
                             thriftClass);
  }

  @Override
  public RecordWriter<M, ThriftWritable<M>> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = ThriftUtils.getTypeRef(HadoopCompat.getConfiguration(job), LzoThriftB64LineOutputFormat.class);
    }
    return new LzoBinaryB64LineRecordWriter<M, ThriftWritable<M>>(new ThriftConverter<M>(typeRef_), getOutputStream(job));
  }
}
