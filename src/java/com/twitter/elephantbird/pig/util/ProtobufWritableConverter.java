package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
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

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWritableConverter.class);

  protected final TypeRef<M> typeRef;
  protected final ProtobufToPig protobufToPig;
  protected List<FieldDescriptor> extensionDescriptors;

  public ProtobufWritableConverter(String protobufClassName) {
    this(protobufClassName, null);
  }

  public ProtobufWritableConverter(String protoClassName,
      String[] protoExtensionNames) {
    super(new ProtobufWritable<M>());

    Preconditions.checkNotNull(protoClassName);
    typeRef = PigUtil.getProtobufTypeRef(protoClassName);
    protobufToPig = new ProtobufToPig();
    if (protoExtensionNames !=null) {
      setExtentionNames(protoExtensionNames);
    }
    writable.setConverter(typeRef.getRawClass());
  }

  @SuppressWarnings("unchecked")
  private void setExtentionNames(String[] protoExtensionNames) {
    List<GeneratedExtension<M, ?>> protoExtensions = new ArrayList<GeneratedExtension<M,?>>();
    List<FieldDescriptor> fds = new ArrayList<FieldDescriptor>();

    for(String e: protoExtensionNames) {
      String enclosingClassName = e.substring(0, e.lastIndexOf('.'));
      String extensionName = e.substring(e.lastIndexOf('.') + 1);
      try {
        Class<?> enclosingClass = Class.forName(enclosingClassName);
        GeneratedExtension<M, ?> extension = (GeneratedExtension<M, ?>) enclosingClass.getField(extensionName).get(null);
        fds.add(extension.getDescriptor());
        protoExtensions.add(extension);
      } catch (Exception ex) {
        LOG.error(ex.toString(), ex);
      }
    }
    writable.setExtensions(protoExtensions);
    extensionDescriptors = fds;
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
    if (extensionDescriptors != null) {
      for(FieldDescriptor fd: extensionDescriptors) {
        FieldSchema fs = null;
        if(fd.getType() == FieldDescriptor.Type.MESSAGE) {
          fs = protobufToPig.messageToFieldSchema(fd);
        } else {
          fs = protobufToPig.singleFieldToFieldSchema(fd);
        }
        schema.add(fs);
      }
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
    //TODO:
    return protobufToPig.toTuple(writable.get());
  }

  @Override
  protected ProtobufWritable<M> toWritable(Tuple value) throws IOException {
    writable.set(PigToProtobuf.tupleToMessage(typeRef.getRawClass(), value));
    return writable;
  }
}
