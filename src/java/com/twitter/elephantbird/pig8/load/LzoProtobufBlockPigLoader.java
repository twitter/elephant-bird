package com.twitter.elephantbird.pig8.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig8.util.ProtobufToPig;
import com.twitter.elephantbird.pig8.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;


public abstract class LzoProtobufBlockPigLoader<M extends Message> extends LzoBaseLoadFunc implements LoadMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockPigLoader.class);

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
    try {
      while ( reader_.nextKeyValue()) {
        value_ = (ProtobufWritable<M>) reader_.getCurrentValue();
        t = new ProtobufTuple(value_.get());
        incrCounter(LzoProtobufBlockPigLoaderCounters.ProtobufsRead, 1L);
        return t;
      }
    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    }
    return t;
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    return new ResourceSchema(protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass())));
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    return null;
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public void setPartitionFilter(Expression expr) throws IOException {
  }
}
