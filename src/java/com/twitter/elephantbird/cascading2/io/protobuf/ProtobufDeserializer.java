package com.twitter.elephantbird.cascading2.io.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

import com.google.protobuf.Message;

import org.apache.hadoop.io.serializer.Deserializer;

/**
 * Deserializes delimited protobufs from input stream
 * @author Ning Liang
 */
public class ProtobufDeserializer implements Deserializer<Message> {

  private Method parseMethod;
  private InputStream in;

  public ProtobufDeserializer(Class<Message> klass) {
    parseMethod = ProtobufReflectionUtil.parseMethodFor(klass);
  }

  @Override
  public void open(InputStream inStream) throws IOException {
    in = inStream;
  }

  @Override
  public Message deserialize(Message message) throws IOException {
    return ProtobufReflectionUtil.parseMessage(parseMethod, in);
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

}
