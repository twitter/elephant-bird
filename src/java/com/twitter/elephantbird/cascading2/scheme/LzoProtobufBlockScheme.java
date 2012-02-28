package com.twitter.elephantbird.cascading2.scheme;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;

/**
 * Scheme for Protobuf block encoded files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoProtobufBlockScheme extends
  Scheme<HadoopFlowProcess, JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  private static final long serialVersionUID = -5011096855302946105L;
  private Class protoClass;

  public LzoProtobufBlockScheme(Class protoClass) {
    this.protoClass = protoClass;
  }

  @Override
  public void sink(HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
    throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public void sinkConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    throw new NotImplementedException();
  }

  @Override
  public boolean source(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    //Read the next item into length 2 object array (key, value):
    Object[] context = sourceCall.getContext();
    if (!sourceCall.getInput().next(context[0], context[1])) {
      return false;
    }
    //We have the next value, decode it:
    ProtobufWritable writable = (ProtobufWritable) context[1];
    //getTuple returns a tuple with the length of the Fields size
    sourceCall.getIncomingEntry().setTuple(new Tuple(writable.get()));
    //Only successful exit point is here:
    return true;
  }

  @Override
  public void sourceCleanup(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  @Override
  public void sourceConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    conf.setInputFormat(
      DeprecatedLzoProtobufBlockInputFormat.getInputFormatClass(protoClass, conf));
  }

  /**
  * This sets up the state between succesive calls to source
  */
  @Override
  public void sourcePrepare(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) {
    //Hadoop sets a key value pair:
    sourceCall.setContext(new Object[2]);
    sourceCall.getContext()[0] = sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = sourceCall.getInput().createValue();
  }
}
