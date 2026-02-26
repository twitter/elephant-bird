package com.twitter.elephantbird.cascading2.scheme;

import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Scheme for Protobuf B64 line encoded files.
 * @deprecated please use {@link LzoProtobufScheme}
 * @author Argyris Zymnis
 */
@Deprecated
public class LzoProtobufB64LineScheme<M extends Message> extends
  LzoProtobufScheme<M> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LineScheme.class);
  public LzoProtobufB64LineScheme(Class protoClass) {
    super(protoClass);
    LOG.warn("LzoProtobufB64LineScheme is deprecated, please use LzoProtobufScheme");
  }
}
