package com.twitter.elephantbird.mapreduce.io;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.twitter.elephantbird.util.TypeRef;

public class ThriftConverter<M extends TBase<?>> implements BinaryProtoConverter<M> {
  
  private TypeRef<M> typeRef;
  private TSerializer serializer;
  private TDeserializer deserializer;
  
  public ThriftConverter(TypeRef<M> typeRef) {
    this.typeRef = typeRef;
  }
      
  @Override
  public M fromBytes(byte[] messageBuffer) {
    if (deserializer == null)
      deserializer = new TDeserializer();
    try {
      M message = typeRef.safeNewInstance();
      deserializer.deserialize(message, messageBuffer);
      return message;
    } catch (TException e) {
      // print a warning?
      return null;
    } 
  }

  @Override
  public byte[] toBytes(M message) {
    if (serializer == null)
      serializer = new TSerializer();
    try {
      return serializer.serialize(message);
    } catch (TException e) {
      return null;
    }
  }
}
