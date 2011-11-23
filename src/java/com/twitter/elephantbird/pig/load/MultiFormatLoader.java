package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.twitter.elephantbird.mapreduce.input.LzoRecordReader;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A loader based on {@link MultiInputFormat} to load input with
 * different file formats.
 *
 * @see MultiInputFormat
 */
public class MultiFormatLoader<M> extends LzoBaseLoadFunc {

  private TypeRef<M> typeRef = null;
  private ObjToTuple tupleImpl;

  // Pig requires a default constructor. Do not use.
  public MultiFormatLoader() {
  }

  /**
   * @param className Thrift or Protobuf class
   */
  public MultiFormatLoader(String className) {
    Class<?> clazz = PigUtil.getClass(className);
    typeRef = new TypeRef<M>(clazz){};

    // initialize tupleImpl
    if (Message.class.isAssignableFrom(clazz)) {
      tupleImpl = new ProtobufToTuple();
    } else if (TBase.class.isAssignableFrom(clazz)) {
      tupleImpl = new ThriftToTuple();
    } else {
      throw new RuntimeException(className + " is not a Protobuf or Thrift class");
    }
  }

  /**
   * Return next Tuple from input.
   * <p>
   * A small fraction of bad records in input are tolerated.
   * See  {@link LzoRecordReader} for more information on error handling.
   */
  public Tuple getNext() throws IOException {
    M value = getNextBinaryValue(typeRef);

    return value != null ?
        tupleImpl.toTuple(value) : null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    return tupleImpl.getSchema();
  }

  @Override
  public InputFormat<LongWritable, BinaryWritable<M>> getInputFormat() throws IOException {
    return new MultiInputFormat<M>(typeRef);
  }

  // END OF Pig Loader implementation

  /*
   * classes to convert to Thrift or Protobuf object into tuples:
   */
  private static interface ObjToTuple {
    abstract ResourceSchema getSchema();
    abstract Tuple toTuple(Object obj);
  };

  private class ThriftToTuple implements ObjToTuple {

    @SuppressWarnings("unchecked")
    private ThriftToPig<TBase<?, ?>> thriftToPig = ThriftToPig.newInstance((TypeRef)typeRef);

    @Override
    public ResourceSchema getSchema() {
      return new ResourceSchema(thriftToPig.toSchema());
    }

    @Override
    public Tuple toTuple(Object obj) {
      return thriftToPig.getLazyTuple((TBase<?, ?>)obj);
    }
  }

  private class ProtobufToTuple implements ObjToTuple {

    @Override @SuppressWarnings("unchecked")
    public ResourceSchema getSchema() {
      Descriptor desc = Protobufs.getMessageDescriptor((Class<Message>)typeRef.getRawClass());
      return new ResourceSchema(new ProtobufToPig().toSchema(desc));
    }

    @Override
    public Tuple toTuple(Object obj) {
      return new ProtobufTuple((Message)obj);
    }
  }
}
