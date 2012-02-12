package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.List;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
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
  protected ProtobufExtensionRegistry<M> protoExtensionRegistry;

  public ProtobufWritableConverter(String protobufClassName) {
    this(protobufClassName, null);
  }

  public ProtobufWritableConverter(String protoClassName,
      String extensionRegistryClassName) {
    super(new ProtobufWritable<M>());

    Preconditions.checkNotNull(protoClassName);
    typeRef = PigUtil.getProtobufTypeRef(protoClassName);
    protobufToPig = new ProtobufToPig();

    ExtensionRegistry extensionRegistry = null;
    if(extensionRegistryClassName != null) {
      @SuppressWarnings("unchecked")
      Class<? extends ProtobufExtensionRegistry<M>> extRegClass =
        (Class<? extends ProtobufExtensionRegistry<M>>) PigUtil.getClass(extensionRegistryClassName);
      protoExtensionRegistry = Protobufs.safeNewInstance(extRegClass);
      extensionRegistry = protoExtensionRegistry.getRealExtensionRegistry();
    }
    writable.setExtensionRegistry(extensionRegistry);
    writable.setConverter(typeRef.getRawClass());
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

    if (protoExtensionRegistry != null) {
      List<FieldDescriptor> fds = Lists.transform(protoExtensionRegistry.getExtensions(),
          new Function<GeneratedExtension<M, ?>, FieldDescriptor>() {
        @Override
        public FieldDescriptor apply(GeneratedExtension<M, ?> extension) {
          return extension.getDescriptor();
        }
      });

      for(FieldDescriptor fd: fds) {
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
