package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.protobuf.Message;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Supports conversion between Pig Tuple and Protocol Buffer types.
 *
 * @author Andy Schlaikjer
 */
public class ProtobufWritableConverter<M extends Message, W extends ProtobufWritable<M>> extends
    AbstractWritableConverter<W> {
  protected final TypeRef<M> typeRef;
  protected final ProtobufToPig protobufToPig;
  protected Class<? extends W> writableClass;

  public ProtobufWritableConverter(String protobufClassName) {
    typeRef = PigUtil.getProtobufTypeRef(protobufClassName);
    protobufToPig = new ProtobufToPig();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(Class<? extends W> writableClass) {
    this.writableClass = writableClass;
    try {
      if (this.writableClass != null) {
        this.writable = writableClass.newInstance();
      } else {
        this.writable = (W) ProtobufWritable.newInstance(typeRef.getRawClass());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    return new ResourceFieldSchema(new FieldSchema(null, protobufToPig.toSchema(Protobufs
        .getMessageDescriptor(typeRef.getRawClass()))));
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToTuple(dataByteArray.get(), null);
  }

  @Override
  protected Tuple toTuple(W writable, ResourceFieldSchema schema) throws IOException {
    return protobufToPig.toTuple(writable.get());
  }

  @Override
  protected W toWritable(Tuple value, boolean newInstance) throws IOException {
    W out = this.writable;
    if (newInstance) {
      try {
        out = writableClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    out.set(PigToProtobuf.tupleToMessage(typeRef.getRawClass(), value));
    return out;
  }
}
