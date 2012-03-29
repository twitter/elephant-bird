package com.twitter.elephantbird.cascading2.scheme;

import com.google.protobuf.Message;

import java.io.IOException;

/**
 * Scheme for Protobuf B64 line encoded files.
 * @deprecated please use {@link LzoProtobufScheme}
 * @author Argyris Zymnis
 */
@Deprecated
public class LzoProtobufB64LineScheme<M extends Message> extends
  LzoProtobufScheme<M> {
  public LzoProtobufB64LineScheme(Class protoClass) {
    super(protoClass);
  }
}
