package com.twitter.elephantbird.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;

/**
 * Extends TDeserializer to improve handling of corrupt records in a few ways:
 *
 * <ul>
 * <li> sets read-limit for TBinaryProtocol before each deserialization.
 *      Reduces OutOfMemoryError exceptions.
 *
 * <li> {@link ThriftBinaryProtocol} to avoid excessive cpu consumed while
 *      skipping some corrupt records.
 *
 * <li> {@code deserialize(buf, offset, len)} method can avoid buffer copies.
 *      Serialized struct need not span a entire byte array.
 * </ul>
 */
public class ThriftBinaryDeserializer extends TDeserializer {

  // use protocol and transport directly instead of using ones in TDeserializer
  private final TMemoryInputTransport trans = new TMemoryInputTransport();
  private final TBinaryProtocol protocol = new ThriftBinaryProtocol(trans);

  public ThriftBinaryDeserializer() {
    super(new ThriftBinaryProtocol.Factory());
  }

  @Override
  public void deserialize(TBase base, byte[] bytes) throws TException {
    deserialize(base, bytes, 0, bytes.length);
  }

  /**
   * Same as {@link #deserialize(TBase, byte[])}, but much more buffer copy friendly.
   */
  public void deserialize(TBase base, byte[] bytes, int offset, int len) throws TException {
    protocol.reset();
    protocol.setReadLength(len); // reduces OutOfMemoryError exceptions
    trans.reset(bytes, offset, len);
    base.read(protocol);
  }
}
