package com.twitter.elephantbird.pig.load;

import com.google.protobuf.Message;

/**
 * Loader for LZO-compressed files written using the ProtobufBlockInputFormat<br>
 * Initialize with a String argument that represents the full classpath of the protocol buffer class to be loaded.<br>
 * The no-arg constructor will not work and is only there for internal Pig reasons.
 * @param <M>
 */
public class LzoProtobufBlockPigLoader<M extends Message> extends LzoProtobufB64LinePigLoader<M> {

  /**
   * Default constructor. Do not use for actual loading.
   */
  public LzoProtobufBlockPigLoader() {
  }

  /**
   * @param protoClassName full classpath to the generated Protocol Buffer to be loaded.
   */
  public LzoProtobufBlockPigLoader(String protoClassName) {
    super(protoClassName);
  }
}