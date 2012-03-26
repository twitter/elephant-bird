package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedLzoThriftBlockOutputFormat;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;

import org.apache.thrift.TBase;

import java.io.IOException;

/**
 * Scheme for Thrift block encoded files.
 *
 * @author Argyris Zymnis
 */
public class LzoThriftScheme<M extends TBase<?,?>> extends
  LzoBinaryScheme<M, ThriftWritable<M>> {

  private static final long serialVersionUID = -5011096855302946109L;
  private Class thriftClass;

  public LzoThriftScheme(Class thriftClass) {
    this.thriftClass = thriftClass;
  }

  @Override
  public void sinkConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    conf.setOutputFormat(
      DeprecatedLzoThriftBlockOutputFormat.getOutputFormatClass(thriftClass, conf)
    );
  }

  protected ThriftWritable<M> prepareBinaryWritable() {
    TypeRef<M> typeRef = (TypeRef<M>) ThriftUtils.getTypeRef(thriftClass);
    return new ThriftWritable(typeRef);
  }

  @Override
  public void sourceConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    MultiInputFormat.setClassConf(thriftClass, conf);
    DeprecatedInputFormatWrapper.setInputFormat(MultiInputFormat.class, conf);
  }
}
