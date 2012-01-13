package com.twitter.elephantbird.cascading2.scheme;

import com.google.protobuf.Message;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;

/**
 * Scheme for Protobuf/Base64 encoded log files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoProtobufB64LineScheme extends LzoB64LineScheme {

  private transient BinaryConverter<Message> converter;
  private Class protoClass;

  public LzoProtobufB64LineScheme(Class protoClass) {
    this.protoClass = protoClass;
  }

  protected BinaryConverter<Message> getConverter() {
    if (converter == null) {
      converter = ProtobufConverter.newInstance(protoClass);
    }
    return converter;
  }

  protected Object decodeMessage(byte[] bytes) {
    return getConverter().fromBytes(bytes);
  }

  protected byte[] encodeMessage(Object message) {
    return getConverter().toBytes((Message) message);
  }
}
