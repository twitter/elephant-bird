package com.twitter.elephantbird.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * extends TDeserializer in order to set read-limit for
 * underlying TBinaryProtocol before each deserialization. <p>
 *
 * This improves handling of deserialization errors.
 * Otherwise, when the input is corrupt it can result in OutOfMemoryError
 * or other exceptions rather than a TException.
 */
public class ThriftBinaryDeserializer extends TDeserializer {

  private final TBinaryProtocol protocol;

  /**
   * @see ThriftBinaryDeserializer
   */
  public ThriftBinaryDeserializer() {
    this(new Factory()); //second constructor to access the Factory object
  }

  private ThriftBinaryDeserializer(Factory factory) {
    super(factory);
    protocol = factory.protocol;
  }

  /** stores protocol returned by super.getProtocol() */
  private static class Factory extends TBinaryProtocol.Factory {
    TBinaryProtocol protocol = null;

    @Override
    public TProtocol getProtocol(TTransport trans) {
      protocol = (TBinaryProtocol) super.getProtocol(trans);
      return protocol;
    }
  }

  @Override
  public void deserialize(TBase base, byte[] bytes) throws TException {
    protocol.setReadLength(bytes.length); // the class exists to do this
    super.deserialize(base, bytes);
  }
}
