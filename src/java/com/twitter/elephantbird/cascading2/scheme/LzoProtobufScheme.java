package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.google.protobuf.Message;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;

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
  public void sinkConfInit(FlowProcess<JobConf> hfp, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    LzoProtobufBlockOutputFormat.setClassConf(protoClass, conf);
    DeprecatedOutputFormatWrapper.setOutputFormat(LzoProtobufBlockOutputFormat.class, conf);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> hfp, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    MultiInputFormat.setClassConf(protoClass, conf);
    DeprecatedInputFormatWrapper.setInputFormat(MultiInputFormat.class, conf);
  }
}
