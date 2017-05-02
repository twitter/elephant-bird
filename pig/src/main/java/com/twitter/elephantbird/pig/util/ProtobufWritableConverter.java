package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
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
  protected final Descriptor descriptor;
  protected final Schema expectedSchema;

  public ProtobufWritableConverter(String protobufClassName) {
    super(new ProtobufWritable<M>());
    Preconditions.checkNotNull(protobufClassName);
    typeRef = PigUtil.getProtobufTypeRef(protobufClassName);
    protobufToPig = new ProtobufToPig();
    writable.setConverter(typeRef.getRawClass());
    descriptor = PigUtil.getProtobufDescriptor(typeRef.getRawClass());
    expectedSchema = protobufToPig.toSchema(descriptor);
  }

  @Override
  public void initialize(Class<? extends ProtobufWritable<M>> writableClass) throws IOException {
    if (writableClass == null) {
      return;
    }
    super.initialize(writableClass);
    writable.setConverter(typeRef.getRawClass());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    return new ResourceFieldSchema(new FieldSchema(null, protobufToPig.toSchema(Protobufs
        .getMessageDescriptor(typeRef.getRawClass()))));
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    Preconditions.checkNotNull(schema, "Schema is null");
    Preconditions.checkArgument(DataType.TUPLE == schema.getType(),
        "Expected schema type '%s' but found type '%s'", DataType.findTypeName(DataType.TUPLE),
        DataType.findTypeName(schema.getType()));
    ResourceSchema childSchema = schema.getSchema();
    Preconditions.checkNotNull(childSchema, "Child schema is null");
    Schema actualSchema = Schema.getPigSchema(childSchema);
    Preconditions.checkArgument(Schema.equals(expectedSchema, actualSchema, false, true),
        "Expected store schema '%s' but found schema '%s'", expectedSchema, actualSchema);
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
