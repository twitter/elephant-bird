package com.twitter.elephantbird.cascading2.scheme;

import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Scheme for Protobuf block encoded files.
 * @deprecated please use {@link LzoProtobufScheme}
 * @author Argyris Zymnis
 */
@Deprecated
public class LzoProtobufBlockScheme<M extends Message> extends
  LzoProtobufScheme<M> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockScheme.class);
  public LzoProtobufBlockScheme(Class protoClass) {
    super(protoClass);
    LOG.warn("LzoProtobufBlockScheme is deprecated, please use LzoProtobufScheme");
  }
}
