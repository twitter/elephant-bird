package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.util.ThriftUtils;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TBase;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class DeprecatedLzoThriftBlockInputFormat<M extends TBase<?,?>> extends DeprecatedLzoInputFormat<M, BinaryWritable<M>> {
  private TypeRef typeRef_;
  private BinaryWritable<M> thriftWritable_;

  public void setTypeRef(TypeRef typeRef) {
    typeRef_ = typeRef;
  }

  protected void setThriftWritable(ThriftWritable<M> thriftWritable) {
    thriftWritable_ = thriftWritable;
  }

  /**
   * Returns {@link DeprecatedLzoProtobufBlockInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("unchecked")
  public static <M extends TBase<?,?>> Class<DeprecatedLzoThriftBlockInputFormat>
     getInputFormatClass(Class<M> thriftClass, JobConf jobConf) {
    ThriftUtils.setClassConf(jobConf, DeprecatedLzoThriftBlockInputFormat.class, thriftClass);
    return DeprecatedLzoThriftBlockInputFormat.class;
  }

  @Override
  public RecordReader<M, BinaryWritable<M>> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
    if (typeRef_ == null) {
      typeRef_ = ThriftUtils.getTypeRef(jobConf, DeprecatedLzoThriftBlockInputFormat.class);
    }
    if(thriftWritable_ == null) {
      thriftWritable_ = new ThriftWritable<M>(typeRef_);
    }
    return new DeprecatedLzoThriftBlockRecordReader(typeRef_, thriftWritable_, jobConf, (FileSplit) inputSplit);
  }
}
