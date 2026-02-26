package com.twitter.elephantbird.mapreduce.io;

import java.io.IOException;

/**
 * Thrown by BinaryConverter if it fails to deserialize bytes.
 */
public class DecodeException extends IOException {
  public DecodeException(Throwable cause) {
    super("BinaryConverter failed to decode", cause);
  }
}
