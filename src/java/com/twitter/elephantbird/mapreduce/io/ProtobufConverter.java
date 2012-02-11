package com.twitter.elephantbird.mapreduce.io;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
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
  private List<GeneratedExtension<M, ?>> protoExtensions;
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
    return ProtobufConverter.newInstance(protoClass, null);
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(
      TypeRef<M> typeRef) {
    return ProtobufConverter.newInstance(typeRef, null);
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(
      Class<M> protoClass, List<GeneratedExtension<M, ?>> protoExtensions) {
    return ProtobufConverter.newInstance(new TypeRef<M>(protoClass){},
        protoExtensions);
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(
      TypeRef<M> typeRef, List<GeneratedExtension<M, ?>> protoExtensions) {
    return new ProtobufConverter<M>(typeRef, protoExtensions);
  }

  public ProtobufConverter(TypeRef<M> typeRef) {
    this.typeRef = typeRef;
  }

  public ProtobufConverter(TypeRef<M> typeRef, List<GeneratedExtension<M, ?>> protoExtensions) {
    this(typeRef);
    this.protoExtensions = protoExtensions;
  }


  @SuppressWarnings("unchecked")
  @Override
  public M fromBytes(byte[] messageBuffer) {
    try {
      if (protoBuilder == null) {
        protoBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
      }
      if (protoExtensions != null && extensionRegistry == null) {
        initExtensionRegistry();
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

  private void initExtensionRegistry() {
    ExtensionRegistry registry = ExtensionRegistry.newInstance();
    for(GeneratedExtension<M, ?> e: protoExtensions) {

//      GeneratedExtension<M, ?> extensionObj = null;
//      for(Field f: e.getFields()) {
//        if (GeneratedExtension.class.isAssignableFrom(f.getType())) {
//          try {
//            extensionObj = (GeneratedExtension<M, ?>) f.get(null);
//          } catch (IllegalAccessException ex) {
//            logWarning("Fail to get protobuf extension field " + f.getName(), ex);
//            continue;
//          }
//          registry.add(extensionObj);
//        }
//      }
      registry.add(e);
    }

    extensionRegistry = registry;
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
      if(protoExtensions != null) {
        ret = protoExtensions.equals(rhs.protoExtensions);
      } else {
        ret = rhs.protoExtensions == null;
      }
    } catch (ClassCastException e) {
    }
    return ret;
  }

  @Override
  public int hashCode() {
	int hashCode = 7;
	hashCode = 31 * hashCode + typeRef.getType().hashCode();
	hashCode = 31 * hashCode + (protoExtensions == null ? 0 :
	  protoExtensions.hashCode());

	return hashCode;
  }
}
