package com.twitter.elephantbird.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;

/**
 * ThriftCompat is used to be a bridge between thrift cross version compatible code, which can leave in the main source
 * directory, and isolated incompatible code.
 *
 * Version incompatible code should be isolated in ThriftCompat under folder thrift7 or thrift9, depending for what version
 * your specific code is.
 */
public class ThriftCompat {

  public static ThriftBinaryDeserializer createBinaryDeserializer() {
    return new ThriftBinaryDeserializer() {
      protected void resetAndInitialize(TBinaryProtocol protocol, int newLength) {
        super.resetAndInitialize(protocol, newLength);
        protocol.setReadLength(newLength); // reduces OutOfMemoryError exceptions
      }
    };
  }

  public static ThriftBinaryProtocol createBinaryProtocol(TTransport tTransport) {
    return new ThriftBinaryProtocol(tTransport) {
      /**
       * NOTE: This assumes that the elements are one byte each.
       * So this does not catch all cases, but does increase the chances of
       * handling malformed lengths when the number of remaining bytes in
       * the underlying Transport is clearly less than the container size
       * that the Transport provides.
       */
      protected void checkContainerSize(int size) throws TProtocolException {
        super.checkContainerSize(size);
        if (checkReadLength_ && (readLength_ - size) < 0) {

          throw new TProtocolException(
            "Remaining message length is " + readLength_ + " but container size in underlying TTransport is set to at least: " + size
          );
        }
      }
    };
  }
}
