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
  
  private Message.Builder protoBuilder; 
  private TypeRef<M> typeRef;
  
  public ProtobufConverter(TypeRef<M> typeRef) {
    this.typeRef = typeRef;
  }
  
  @SuppressWarnings("unchecked") 
  @Override
  public M fromBytes(byte[] messageBuffer) {
    try {
      if (protoBuilder == null) {
        protoBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
      }
      return  (M) protoBuilder.clone().mergeFrom(messageBuffer).build();
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Invalid Protocol Buffer exception building " + typeRef.getRawClass().getName(), e);
    } catch(UninitializedMessageException ume) {
      LOG.error("Uninitialized Message Exception in building " + typeRef.getRawClass().getName(), ume);
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
   
    if (!(obj instanceof ProtobufConverter<?>))
      return false;
    
    return typeRef.getType().equals(((ProtobufConverter<?>)obj).typeRef.getType());
  }
}
