package com.twitter.elephantbird.cascading3.scheme;

import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;

import org.apache.thrift.TBase;

/**
 * Scheme for Thrift lzo compressed files.
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
  public void sinkConfInit(FlowProcess<? extends Configuration> hfp, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
    LzoThriftBlockOutputFormat.setClassConf(thriftClass, conf);
    conf.setClass("mapreduce.outputformat.class", LzoThriftBlockOutputFormat.class, OutputFormat.class);
  }

  protected ThriftWritable<M> prepareBinaryWritable() {
    TypeRef<M> typeRef = (TypeRef<M>) ThriftUtils.getTypeRef(thriftClass);
    return new ThriftWritable(typeRef);
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> hfp, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
    MultiInputFormat.setClassConf(thriftClass, conf);
    DelegateCombineFileInputFormat.setDelegateInputFormatHadoop2(conf, MultiInputFormat.class);
  }
}
