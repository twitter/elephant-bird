package com.twitter.elephantbird.pig.load;

import com.google.protobuf.Message;

/**
 * @Deprecated use {@link ProtobufPigLoader}
 */
public class LzoProtobufB64LinePigLoader<M extends Message> extends ProtobufPigLoader<M> {

  public LzoProtobufB64LinePigLoader(String protoClassName) {
    super(protoClassName);
    LOG.warn("LzoProtobufB64LinePigLoader is deprecated and will be removed in future. " +
             "please use ProtobufPigLoader");
  }
}