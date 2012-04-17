package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
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

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWritableConverter.class);

  protected final TypeRef<M> typeRef;
  protected final ProtobufToPig protobufToPig;
  private ProtobufExtensionRegistry extensionRegistry;
  protected final Descriptor descriptor;
  protected final Schema expectedSchema;

  public ProtobufWritableConverter(String protobufClassName) {
    this(protobufClassName, null);
  }

  public ProtobufWritableConverter(String protoClassName,
      String extensionRegistryClassName) {
    Preconditions.checkNotNull(protoClassName);

    typeRef = PigUtil.getProtobufTypeRef(protoClassName);
    setWritable(new ProtobufWritable<M>(typeRef));

    if(extensionRegistryClassName != null) {
      extensionRegistry = Protobufs.getExtensionRegistry(extensionRegistryClassName);
    }
    protobufToPig = new ProtobufToPig();

    writable.setExtensionRegistry(extensionRegistry);
    writable.setConverter(typeRef.getRawClass());
    descriptor = PigUtil.getProtobufDescriptor(typeRef.getRawClass());
    expectedSchema = protobufToPig.toSchema(descriptor, extensionRegistry);
  }

  @Override
  public void initialize(Class<? extends ProtobufWritable<M>> writableClass) throws IOException {
    if (writableClass == null) {
      return;
    }
    super.initialize(writableClass);
    writable.setExtensionRegistry(extensionRegistry);
    writable.setConverter(typeRef.getRawClass());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    return new ResourceFieldSchema(new FieldSchema(null,
        protobufToPig.toSchema(Protobufs.getMessageDescriptor(
            typeRef.getRawClass()), extensionRegistry)));
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
    return protobufToPig.toTuple(writable.get(), extensionRegistry);
  }

  @Override
  protected ProtobufWritable<M> toWritable(Tuple value) throws IOException {
    writable.set(PigToProtobuf.tupleToMessage(typeRef.getRawClass(), value, extensionRegistry));
    return writable;
  }
}
