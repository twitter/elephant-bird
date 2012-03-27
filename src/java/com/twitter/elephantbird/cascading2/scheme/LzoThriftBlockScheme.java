package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoThriftMultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;

/**
 * Scheme for Thrift block encoded files.
 *
 * @author Argyris Zymnis
 */
public class LzoThriftBlockScheme extends
  LzoBlockScheme<ThriftWritable<?>> {

  private static final long serialVersionUID = -5011096855302946109L;
  private Class thriftClass;

  public LzoThriftBlockScheme(Class thriftClass) {
    this.thriftClass = thriftClass;
  }

  @Override
  public void sourceConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    conf.setInputFormat(
      DeprecatedLzoThriftMultiInputFormat.getInputFormatClass(thriftClass, conf));
  }
}
