package com.twitter.elephantbird.thrift;

import org.apache.thrift.transport.TTransport;

/**
 * ThriftCompat is used to be a bridge between thrift cross version compatible code, which can live in the main source
 * directory, and isolated incompatible code.
 *
 * Version incompatible code should be isolated in ThriftCompat under folder thrift7 or thrift9, depending for what version
 * your specific code is.
 */
public class ThriftCompat {
  public static ThriftBinaryDeserializer createBinaryDeserializer() {
    return new ThriftBinaryDeserializer();
  }

  public static ThriftBinaryProtocol createBinaryProtocol(TTransport tTransport) {
    return new ThriftBinaryProtocol(tTransport);
  }
}
