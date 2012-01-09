package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import java.lang.reflect.Method;
import java.lang.reflect.Field;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.GeneratedMessage.ExtendableMessage;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.Descriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
    
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWritableConverter.class);
  
  protected final TypeRef<M> typeRef;
  protected final ProtobufToPig protobufToPig;
  protected FieldDescriptor extensionDescriptor;

  public ProtobufWritableConverter(String protobufClassName) {
    super(new ProtobufWritable<M>());
    int index = protobufClassName.indexOf(' ');
    String protobufExtensionClassName = null;
    if (index >=0) {
      protobufExtensionClassName = protobufClassName.substring(index + 1);
      protobufClassName = protobufClassName.substring(0, index);
    }
    Preconditions.checkNotNull(protobufClassName);
    typeRef = PigUtil.getProtobufTypeRef(protobufClassName);
    protobufToPig = new ProtobufToPig();
    if (protobufExtensionClassName !=null) {
      setExtentionClassName(protobufExtensionClassName);
    }
    writable.setConverter(typeRef.getRawClass());
  }
  
  private void setExtentionClassName(String protobufExtensionClassName) {
    try {
      Class<?> extensionClass = Class.forName(protobufExtensionClassName);
      writable.setExtensionClass(extensionClass);
      Method m = extensionClass.getMethod("getDescriptor", new Class[]{});
      FileDescriptor fd = (FileDescriptor) m.invoke(null, new Object[]{});
      extensionDescriptor = fd.getExtensions().get(0);
    } catch (Exception ex) {
      LOG.error(ex.toString(), ex);
    }
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
    Schema schema = protobufToPig.toSchema(Protobufs.getMessageDescriptor(typeRef.getRawClass()));
    if (extensionDescriptor != null) {
      FieldSchema extensionSchema = protobufToPig.messageToFieldSchema(extensionDescriptor);
      schema.add(extensionSchema);
    }
    return new ResourceFieldSchema(new FieldSchema(null, schema));
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
