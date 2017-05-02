package com.twitter.elephantbird.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;

/**
 * A shim on top of thrift to allow for thrift 0.7/0.9 compatibility.
 * 
 * This one is designed for thrift 0.7
 *
 */
abstract class AbstractThriftBinaryProtocol extends TBinaryProtocol {
  public AbstractThriftBinaryProtocol(TTransport trans) {
    super(trans);
  }

  public AbstractThriftBinaryProtocol(TTransport trans, boolean strictRead, boolean strictWrite) {
    super(trans, strictRead, strictWrite);
  }

  protected void resetAndInitialize(TBinaryProtocol protocol, int newLength) {
    protocol.reset();
    protocol.setReadLength(newLength);
  }

  /**
   * Check if the container size is valid.
   * 
   * NOTE: This assumes that the elements are one byte each. So this does not
   * catch all cases, but does increase the chances of handling malformed
   * lengths when the number of remaining bytes in the underlying Transport is
   * clearly less than the container size that the Transport provides.
   */
  protected void checkContainerSize(int size) throws TProtocolException {
    if (size < 0) {
      throw new TProtocolException("Negative container size: " + size);
    }

    if (checkReadLength_ && (readLength_ - size) < 0) {
      throw new TProtocolException("Remaining message length is " + readLength_
              + " but container size in underlying TTransport is set to at least: " + size);
    }
  }
}
