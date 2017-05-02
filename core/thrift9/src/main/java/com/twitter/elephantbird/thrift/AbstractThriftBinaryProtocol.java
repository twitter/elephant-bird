package com.twitter.elephantbird.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;

/**
 * A shim on top of thrift to allow for thrift 0.7/0.9 compatibility.
 * 
 * This one is designed for thrift 0.9 and above
 *
 */
abstract class AbstractThriftBinaryProtocol extends TBinaryProtocol {
  public AbstractThriftBinaryProtocol(TTransport trans) {
    super(trans);
  }

  /**
   * Check if the container size is valid.
   */
  protected void checkContainerSize(int size) throws TProtocolException {
    if (size < 0) {
      throw new TProtocolException("Negative container size: " + size);
    }
  }
}
