package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;

/**
 * Reads Thrift objects written in blocks using LzoThriftBlockOutputFormat
 * <br><br>
 *
 * Do not use LzoThriftBlockInputFormat.class directly for setting
 * InputFormat class for a job. Use getInputFormatClass() instead.
 */
public class LzoThriftBlockInputFormat<M extends TBase<?, ?>>
                extends LzoInputFormat<LongWritable, ThriftWritable<M>> {
  // implementation is exactly same as LzoThriftB64LineINputFormat

  private TypeRef<M> typeRef_;

  public LzoThriftBlockInputFormat() {}

  public LzoThriftBlockInputFormat(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  /**
   * Returns {@link LzoThriftBlockInputFormat} class for setting up a job.
   * Sets an internal configuration in jobConf so that Task instantiates
   * appropriate object for this generic class based on thriftClass
   */
  @SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>> Class<LzoThriftBlockInputFormat>
     getInputFormatClass(Class<M> thriftClass, Configuration jobConf) {
    ThriftUtils.setClassConf(jobConf, LzoThriftBlockInputFormat.class, thriftClass);
    return LzoThriftBlockInputFormat.class;
  }

  @Override
  public RecordReader<LongWritable, ThriftWritable<M>> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = ThriftUtils.getTypeRef(taskAttempt.getConfiguration(), LzoThriftBlockInputFormat.class);
    }
    return new LzoThriftBlockRecordReader<M>(typeRef_);
  }

}
