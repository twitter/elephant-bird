package com.twitter.elephantbird.mapreduce.io;

/**
 * A simple interface to serialize and deserialize objects
 */
public interface BinaryProtoConverter<M> {
  /* TODO : What about exceptions?
   * The contract could explicit state that these return null
   * in case of errors?
   */

  M fromBytes(byte[] messageBuffer);
  
  byte[] toBytes(M message);
  
}
