package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.google.protobuf.Message;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedLzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;

import java.io.IOException;

/**
 * Scheme for Protobuf lzo compressed files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoProtobufScheme<M extends Message> extends
  LzoBinaryScheme<M, ProtobufWritable<M>> {

  private static final long serialVersionUID = -5011096855302946105L;
  private Class protoClass;

  public LzoProtobufScheme(Class protoClass) {
    this.protoClass = protoClass;
  }

  protected ProtobufWritable<M> prepareBinaryWritable() {
    TypeRef<M> typeRef = (TypeRef<M>) Protobufs.getTypeRef(protoClass.getName());
    return new ProtobufWritable(typeRef);
  }

  @Override
  public void sinkConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    DeprecatedLzoProtobufBlockOutputFormat.setClassConf(protoClass, conf);
    conf.setOutputFormat(DeprecatedLzoProtobufBlockOutputFormat.class);
  }

  @Override
  public void sourceConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    MultiInputFormat.setClassConf(protoClass, conf);
    DeprecatedInputFormatWrapper.setInputFormat(MultiInputFormat.class, conf);
  }
}
