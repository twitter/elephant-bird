package com.twitter.elephantbird.thrift;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * A shim on top of thrift to allow for thrift 0.7/0.9 compatibility.
 * 
 * This one is designed for thrift 0.9 and above
 *
 */
abstract class AbstractThriftBinaryDeserializer extends TDeserializer {
  public AbstractThriftBinaryDeserializer(TProtocolFactory protocolFactory) {
    super(protocolFactory);
  }

  protected void resetAndInitialize(TBinaryProtocol protocol, int newLength) {
    protocol.reset();
  }
}
