package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
