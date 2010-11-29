package com.twitter.elephantbird.mapreduce.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * {@link BinaryProtoConverter} for Protobufs
 */
public class ProtobufConverter<M extends Message> implements BinaryProtoConverter<M> {
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
}
