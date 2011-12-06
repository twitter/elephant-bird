package com.twitter.elephantbird.cascading.scheme;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Scheme for Protobuf block encoded files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoProtobufBlockScheme extends Scheme {

  private static final long serialVersionUID = -5011096855302946105L;
  private Class protoClass;

  public LzoProtobufBlockScheme(Class protoClass) {
    this.protoClass = protoClass;
  }

  @Override
  public void sink(TupleEntry tuple, OutputCollector output) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public void sinkInit(Tap tap, JobConf conf) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Tuple source(Object key, Object value) {
    ProtobufWritable writable = (ProtobufWritable) value;
    return new Tuple(writable.get());
  }

  @Override
  public void sourceInit(Tap tap, JobConf conf) throws IOException {
    conf.setInputFormat(
      DeprecatedLzoProtobufBlockInputFormat.getInputFormatClass(protoClass, conf));
  }
}
