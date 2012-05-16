package com.twitter.elephantbird.pig.load;

import com.google.protobuf.Message;

/**
 * @Deprecated use {@link ProtobufPigLoader}
 */
public class LzoProtobufBlockPigLoader<M extends Message> extends ProtobufPigLoader<M> {

  public LzoProtobufBlockPigLoader(String protoClassName) {
    super(protoClassName);
    LOG.warn("LzoProtobufBlockPigLoader is deprecated and will be removed in future. " +
             "please use ProtobufPigLoader");
  }
}