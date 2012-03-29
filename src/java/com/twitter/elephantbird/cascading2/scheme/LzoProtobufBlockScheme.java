package com.twitter.elephantbird.cascading2.scheme;

import com.google.protobuf.Message;

import java.io.IOException;

/**
 * Scheme for Protobuf block encoded files.
 * @deprecated please use {@link LzoProtobufScheme}
 * @author Argyris Zymnis
 */
@Deprecated
public class LzoProtobufBlockScheme<M extends Message> extends
  LzoProtobufScheme<M> {
  public LzoProtobufBlockScheme(Class protoClass) {
    super(protoClass);
  }
}
