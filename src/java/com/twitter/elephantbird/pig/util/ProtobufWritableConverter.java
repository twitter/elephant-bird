package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Supports conversion between Pig {@link Tuple} and {@link ProtobufWritable} types. See discussion
 * in {@link ThriftWritableConverter} for example usage.
 *
 * @author Andy Schlaikjer
 * @see ThriftWritableConverter
 */
public class ProtobufWritableConverter<M extends Message> extends
    AbstractWritableConverter<ProtobufWritable<M>> {
  protected final TypeRef<M> typeRef;
  protected final ProtobufToPig protobufToPig;

  public ProtobufWritableConverter(String protobufClassName) {
    super(new ProtobufWritable<M>());
    Preconditions.checkNotNull(protobufClassName);
    typeRef = PigUtil.getProtobufTypeRef(protobufClassName);
    protobufToPig = new ProtobufToPig();
    writable.setConverter(typeRef.getRawClass());
  }

  @Override
  public void initialize(Class<? extends ProtobufWritable<M>> writableClass) {
    if (writableClass == null || writableClass == this.writableClass) {
      return;
    }
    super.initialize(writableClass);
    try {
      writable = writableClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to creat instance of Writable class '%s'",
          writableClass.getName()), e);
    }
    writable.setConverter(typeRef.getRawClass());
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
  protected Tuple toTuple(ProtobufWritable<M> writable, ResourceFieldSchema schema)
      throws IOException {
    return protobufToPig.toTuple(writable.get());
  }

  @Override
  protected ProtobufWritable<M> toWritable(Tuple value) throws IOException {
    writable.set(PigToProtobuf.tupleToMessage(typeRef.getRawClass(), value));
    return writable;
  }
}
