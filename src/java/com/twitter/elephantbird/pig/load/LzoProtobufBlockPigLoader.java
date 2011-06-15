package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Loader for LZO-compressed files written using the ProtobufBlockInputFormat<br>
 * Initialize with a String argument that represents the full classpath of the protocol buffer class to be loaded.<br>
 * The no-arg constructor will not work and is only there for internal Pig reasons.
 * @param <M>
 */
public class LzoProtobufBlockPigLoader<M extends Message> extends LzoBaseLoadFunc {

  private TypeRef<M> typeRef_ = null;
  private final ProtobufToPig protoToPig_ = new ProtobufToPig();

  /**
   * Default constructor. Do not use for actual loading.
   */
  public LzoProtobufBlockPigLoader() {
  }

  /**
   * @param protoClassName full classpath to the generated Protocol Buffer to be loaded.
   */
  public LzoProtobufBlockPigLoader(String protoClassName) {
    TypeRef<M> typeRef = PigUtil.getProtobufTypeRef(protoClassName);
    setTypeRef(typeRef);
  }

  /**
   * Set the type parameter so it doesn't get erased by Java.  Must be called before getNext!
   *
   * @param typeRef
   */
  public void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  public Tuple getNext() throws IOException {
    M value = getNextBinaryValue(typeRef_);

    return value != null ?
        new ProtobufTuple(value) : null;
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    return new ResourceSchema(protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass())));
  }

  @Override
  public InputFormat<LongWritable, ProtobufWritable<M>> getInputFormat() throws IOException {
    return new LzoProtobufBlockInputFormat<M>(typeRef_);
  }
}