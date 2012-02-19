package com.twitter.elephantbird.mapreduce.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * {@link BinaryConverter} for Protobufs
 */
public class ProtobufConverter<M extends Message> implements BinaryConverter<M> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ProtobufConverter.class);

  private Message.Builder protoBuilder;
  private TypeRef<M> typeRef;
  private ExtensionRegistry extensionRegistry;

  // limit the number of warnings in case of serialization errors.
  private static final int MAX_WARNINGS = 100;
  private static int numWarningsLogged = 0;

  private static void logWarning(String message, Throwable t) {
    // does not need to be thread safe
    if ( numWarningsLogged < MAX_WARNINGS ) {
      LOG.info(message, t);
      numWarningsLogged++;
    }
  }

  /**
   * Returns a ProtobufConverter for a given Protobuf class.
   */
  public static <M extends Message> ProtobufConverter<M> newInstance(
      Class<M> protoClass) {
    return ProtobufConverter.newInstance(new TypeRef<M>(protoClass){});
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(
      TypeRef<M> typeRef) {
    return new ProtobufConverter<M>(typeRef);
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(
      Class<M> protoClass, ProtobufExtensionRegistry extensionRegistry) {
    return ProtobufConverter.newInstance(new TypeRef<M>(protoClass){},
        extensionRegistry);
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(
      TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    return new ProtobufConverter<M>(typeRef, extensionRegistry);
  }

  public ProtobufConverter(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public ProtobufConverter(TypeRef<M> typeRef, ProtobufExtensionRegistry protoExtensionRegistry) {
    this.typeRef = typeRef;
    protoBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
    if(protoExtensionRegistry != null) {
//      this.extensionRegistry = ProtobufConverter.getRealExtensionRegistry(
//          protoBuilder.getDescriptorForType(), protoExtensionRegistry);
      this.extensionRegistry = protoExtensionRegistry.getExtensionRegistry();
    }
  }


  @SuppressWarnings("unchecked")
  @Override
  public M fromBytes(byte[] messageBuffer) {
    try {
      if (protoBuilder == null) {
        protoBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
      }

      if (extensionRegistry != null) {
        return (M) protoBuilder.clone().mergeFrom(messageBuffer, extensionRegistry).build();
      }
      return (M) protoBuilder.clone().mergeFrom(messageBuffer).build();
    } catch (InvalidProtocolBufferException e) {
      logWarning("Invalid Protobuf exception while building " +
          typeRef.getRawClass().getName(), e);
    } catch(UninitializedMessageException ume) {
      logWarning("Uninitialized Message Exception while building " +
          typeRef.getRawClass().getName(), ume);
    }
    return null;
  }

  private static ExtensionRegistry getRealExtensionRegistry(Descriptor descriptor,
      ProtobufExtensionRegistry protoExtensionRegistry) {
    ExtensionRegistry extReg = ExtensionRegistry.newInstance();
    ProtobufConverter.populateRealExtensionRegistry(extReg, descriptor, protoExtensionRegistry);
    return extReg;
  }

  private static void populateRealExtensionRegistry(
      ExtensionRegistry realExtensionRegistry,
      FieldDescriptor fieldDescriptor,
      ProtobufExtensionRegistry protoExtensionRegistry) {
    assert(fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE);

    for(GeneratedExtension<?, ?> e: protoExtensionRegistry.getExtensions(fieldDescriptor)) {
      realExtensionRegistry.add(e);
    }

    for(FieldDescriptor e: fieldDescriptor.getMessageType().getFields()) {
      if(e.getType() == FieldDescriptor.Type.MESSAGE) {
        populateRealExtensionRegistry(realExtensionRegistry, e, protoExtensionRegistry);
      }
    }
  }

  private static void populateRealExtensionRegistry(
      ExtensionRegistry realExtensionRegistry,
      Descriptor descriptor,
      ProtobufExtensionRegistry protoExtensionRegistry) {
    for(GeneratedExtension<?, ?> e: protoExtensionRegistry.getExtensions(descriptor)) {
      realExtensionRegistry.add(e);
    }

    for(FieldDescriptor e: descriptor.getFields()) {
      if(e.getType() == FieldDescriptor.Type.MESSAGE) {
        populateRealExtensionRegistry(realExtensionRegistry, e, protoExtensionRegistry);
      }
    }
  }

  @Override
  public byte[] toBytes(M message) {
    return message.toByteArray();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    boolean ret = false;
    ProtobufConverter<?> rhs = (ProtobufConverter<?>)obj;
    try {
      ret = typeRef.getType().equals(rhs.typeRef.getType());
      if(extensionRegistry != null) {
        ret = extensionRegistry.equals(rhs.extensionRegistry);
      } else {
        ret = rhs.extensionRegistry == null;
      }
    } catch (ClassCastException e) {
    }
    return ret;
  }

  @Override
  public int hashCode() {
	int hashCode = 7;
	hashCode = 31 * hashCode + typeRef.getType().hashCode();
	hashCode = 31 * hashCode + (extensionRegistry == null ? 0 :
	  extensionRegistry.hashCode());

	return hashCode;
  }
}
