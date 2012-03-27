package com.twitter.elephantbird.cascading2.scheme;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.BinaryWritable;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Scheme for lzo block encoded files.
 *
 * @author Argyris Zymnis
 */
abstract public class LzoBlockScheme<T extends BinaryWritable<?>> extends
  Scheme<HadoopFlowProcess, JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  private static final Logger LOG = LoggerFactory.getLogger(LzoBlockScheme.class);
  private static final long serialVersionUID = -5011096855302946106L;

  @Override
  public void sink(HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
    throws IOException {
    OutputCollector collector = sinkCall.getOutput();
    TupleEntry entry = sinkCall.getOutgoingEntry();
    T message = (T) entry.getTuple().getObject(0);
    collector.collect(null, message);
  }

  @Override
  public void sinkConfInit(HadoopFlowProcess hfp, Tap tap, JobConf conf) {
    throw new NotImplementedException();
  }

  @Override
  public boolean source(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) throws IOException {

    Object[] context = sourceCall.getContext();
    while(sourceCall.getInput().next(context[0], context[1])) {
      try {
        Object out = ((T) context[1]).get();
        if(out != null) {
          sourceCall.getIncomingEntry().setTuple(new Tuple(out));
          return true;
        }
        LOG.warn("failed to decode record");
      } catch (Throwable e) {
        LOG.warn("while decoding error caught exception: " + e);
      }
    }
    return false;
  }

  @Override
  public void sourceCleanup(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
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
