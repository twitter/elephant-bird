package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;


public abstract class LzoProtobufBlockPigLoader<M extends Message> extends LzoBaseLoadFunc implements LoadMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockPigLoader.class);

  private final ProtobufBlockReader<M> reader_ = null;
  private TypeRef<M> typeRef_ = null;
  private final ProtobufToPig protoToPig_ = new ProtobufToPig();
  private ProtobufWritable<M> value_;
  protected enum LzoProtobufBlockPigLoaderCounters { ProtobufsRead }

  public LzoProtobufBlockPigLoader() {
  }

  /**
   * Set the type parameter so it doesn't get erased by Java.  Must be called before getNext!
   *
   * @param typeRef
   */
  public void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    value_ = new ProtobufWritable<M>(typeRef_);
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader_ == null) {
      return null;
    }

    Tuple t = null;
    if (reader_.readProtobuf(value_)) {
      t = new ProtobufTuple(value_.get());
      incrCounter(LzoProtobufBlockPigLoaderCounters.ProtobufsRead, 1L);
    }
    return t;
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
    return null;
  }

  @Override
  public ResourceSchema getSchema(String arg0, Job arg1) throws IOException {
    return new ResourceSchema(protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass())));
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
    return null;
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public void setPartitionFilter(Expression arg0) throws IOException {
  }
}
