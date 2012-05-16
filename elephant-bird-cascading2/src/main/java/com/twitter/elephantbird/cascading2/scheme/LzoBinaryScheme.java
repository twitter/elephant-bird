package com.twitter.elephantbird.cascading2.scheme;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.BinaryWritable;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Scheme for lzo binary encoded files. Handles both block and base 64. Can be used for Protobuf and Thrift.
 *
 * @author Argyris Zymnis
 */
abstract public class LzoBinaryScheme<M, T extends BinaryWritable<M>> extends
  Scheme<JobConf, RecordReader, OutputCollector, Object[], T> {

  private static final Logger LOG = LoggerFactory.getLogger(LzoBinaryScheme.class);
  private static final long serialVersionUID = -5011096855302946106L;

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<T, OutputCollector> sinkCall)
    throws IOException {
    OutputCollector collector = sinkCall.getOutput();
    TupleEntry entry = sinkCall.getOutgoingEntry();
    T writable = sinkCall.getContext();
    writable.set((M) entry.getTuple().getObject(0));
    collector.collect(null, writable);
  }

  @Override
  public void sinkPrepare( FlowProcess<JobConf> fp, SinkCall<T, OutputCollector> sinkCall ) {
    sinkCall.setContext(prepareBinaryWritable());
  }

  protected abstract T prepareBinaryWritable();

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) throws IOException {

    Object[] context = sourceCall.getContext();
    while(sourceCall.getInput().next(context[0], context[1])) {
      Object out = ((T) context[1]).get();
      if(out != null) {
        sourceCall.getIncomingEntry().setTuple(new Tuple(out));
        return true;
      }
      LOG.warn("failed to decode record");
    }
    return false;
  }

  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  /**
  * This sets up the state between succesive calls to source
  */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) {
    //Hadoop sets a key value pair:
    sourceCall.setContext(new Object[2]);
    sourceCall.getContext()[0] = sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = sourceCall.getInput().createValue();
  }
}
