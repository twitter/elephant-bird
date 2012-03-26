package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;

/**
 * Scheme for Protobuf block encoded files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoProtobufBlockScheme extends
  LzoBlockScheme<ProtobufWritable<?>> {

  private static final long serialVersionUID = -5011096855302946105L;
  private Class protoClass;

  public LzoProtobufBlockScheme(Class protoClass) {
    this.protoClass = protoClass;
  }

  @Override
  public void sourceConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    conf.setInputFormat(
      DeprecatedLzoProtobufBlockInputFormat.getInputFormatClass(protoClass, conf));
  }
}
