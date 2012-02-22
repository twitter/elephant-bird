package com.twitter.elephantbird.cascading2.io.protobuf;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.Message;

import org.apache.hadoop.io.serializer.Serializer;

/**
 * Serializes protobufs with delimiters
 * @author Ning Liang
 */
public class ProtobufSerializer implements Serializer<Message> {

  private OutputStream out;

  @Override
  public void open(OutputStream outStream) throws IOException {
    out = outStream;
  }

  @Override
  public void serialize(Message message) throws IOException {
    message.writeDelimitedTo(out);
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }
}
