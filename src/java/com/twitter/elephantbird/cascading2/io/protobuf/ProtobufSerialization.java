package com.twitter.elephantbird.cascading2.io.protobuf;

import java.util.Comparator;

import com.google.protobuf.Message;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import cascading.tuple.Comparison;

/**
 * Serialization format class
 * @author Ning Liang
 */
public class ProtobufSerialization implements Serialization<Message>, Comparison<Message> {

  @Override
  public boolean accept(Class<?> klass) {
    boolean accept = Message.class.isAssignableFrom(klass);
    return accept;
  }

  @Override
  public Deserializer<Message> getDeserializer(Class<Message> klass) {
    return new ProtobufDeserializer(klass);
  }

  @Override
  public Serializer<Message> getSerializer(Class<Message> klass) {
    return new ProtobufSerializer();
  }

  @Override
  public Comparator<Message> getComparator(Class<Message> klass) {
    return new ProtobufComparator();
  }

}
