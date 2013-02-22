package com.twitter.elephantbird.mapreduce.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * {@link BinaryConverter} for Protobufs
 */
public class ProtobufConverter<M extends Message> implements BinaryConverter<M> {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufConverter.class);

  private Message defaultInstance;
  private TypeRef<M> typeRef;

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
  public static <M extends Message> ProtobufConverter<M> newInstance(Class<M> protoClass) {
    return new ProtobufConverter<M>(new TypeRef<M>(protoClass){});
  }

  public static <M extends Message> ProtobufConverter<M> newInstance(TypeRef<M> typeRef) {
    return new ProtobufConverter<M>(typeRef);
  }

  public ProtobufConverter(TypeRef<M> typeRef) {
    this.typeRef = typeRef;
  }

  @Override
  public M fromBytes(byte[] messageBuffer) {
    return fromBytes(messageBuffer, 0, messageBuffer.length);
  }

  @SuppressWarnings("unchecked")
  public M fromBytes(byte[] messageBuffer, int offset, int len) {
    try {
      if (defaultInstance == null) {
        defaultInstance = Protobufs.getMessageBuilder(typeRef.getRawClass())
                                   .getDefaultInstanceForType();
      }
      return (M) defaultInstance.newBuilderForType()
                                .mergeFrom(messageBuffer, offset, len)
                                .build();
    } catch (InvalidProtocolBufferException e) {
      logWarning("Invalid Protobuf exception while building " + typeRef.getRawClass().getName(), e);
    } catch(UninitializedMessageException ume) {
      logWarning("Uninitialized Message Exception while building " + typeRef.getRawClass().getName(), ume);
    }
    return null;
  }

  @Override
  public byte[] toBytes(M message) {
    return message.toByteArray();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    try {
      return typeRef.getType().equals(((ProtobufConverter<?>)obj).typeRef.getType());
    } catch (ClassCastException e) {
      return false;
    }
  }
}
