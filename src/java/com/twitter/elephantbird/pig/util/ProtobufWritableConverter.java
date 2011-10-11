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
  protected Class<? extends ProtobufWritable<M>> writableClass;

  public ProtobufWritableConverter(String protobufClassName) {
    Preconditions.checkNotNull(protobufClassName);
    typeRef = PigUtil.getProtobufTypeRef(protobufClassName);
    protobufToPig = new ProtobufToPig();
    this.writable = ProtobufWritable.newInstance(typeRef.getRawClass());
  }

  @Override
  public void initialize(Class<? extends ProtobufWritable<M>> writableClass) {
    this.writableClass = writableClass;
    this.writable = createWritable();
  }

  private ProtobufWritable<M> createWritable() {
    if (writableClass != null && ProtobufWritable.class.isAssignableFrom(writableClass)) {
      try {
        ProtobufWritable<M> writable = writableClass.newInstance();
        writable.setConverter(typeRef.getRawClass());
        return writable;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return ProtobufWritable.newInstance(typeRef.getRawClass());
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
  protected ProtobufWritable<M> toWritable(Tuple value, boolean newInstance) throws IOException {
    ProtobufWritable<M> out = this.writable;
    if (newInstance) {
      out = createWritable();
    }
    out.set(PigToProtobuf.tupleToMessage(typeRef.getRawClass(), value));
    return out;
  }
}
