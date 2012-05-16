package com.twitter.elephantbird.mapreduce.io;

/**
 * A simple interface to serialize and deserialize objects
 */
public interface BinaryConverter<M> {
  /* TODO : What about exceptions?
   */

  /** Returns deserialized object. A return of null normally implies an error. */ 
  M fromBytes(byte[] messageBuffer);
  
  byte[] toBytes(M message);
  
}
