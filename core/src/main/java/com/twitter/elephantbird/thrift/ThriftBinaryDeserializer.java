package com.twitter.elephantbird.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;

/**
 * Extends TDeserializer. This implementation improves handling of
 * corrupt records in two ways: <ul>
 *
 * <li> sets read-limit for TBinaryProtocol before each deserialization.
 * Reduces OutOfMemoryError exceptions.
 *
 * <li> Avoids excessive cpu consumed while skipping some corrupt records.
 * </ul>
 */
public class ThriftBinaryDeserializer extends TDeserializer {

  // use protocol and transport directly instead of using ones in TDeserializer
  private final TMemoryInputTransport trans = new TMemoryInputTransport();
  private final TBinaryProtocol protocol = new TBinaryProtocol(trans) {
    // overwrite a few methods so that some malformed messages don't end up
    // taking prohibitively large amounts of cpu in side TProtcolUtil.skip()

    private void checkElemType(byte type) throws TException {
      // only valid types for an element in a container (List, Map, Set)
      // are the ones that are considered in TProtocolUtil.skip()
      switch (type) {
        case TType.BOOL:
        case TType.BYTE:
        case TType.I16:
        case TType.I32:
        case TType.I64:
        case TType.DOUBLE:
        case TType.STRING:
        case TType.STRUCT:
        case TType.MAP:
        case TType.SET:
        case TType.LIST:
          break;

        // list other known types, but not expected
        case TType.STOP:
        case TType.VOID:
        case TType.ENUM: // would be I32 on the wire
        default:
          throw new TException("Unexpected type " + type + " in a container");
      }
    }

    @Override
    public TMap readMapBegin() throws TException {
      TMap map = super.readMapBegin();
      checkElemType(map.keyType);
      checkElemType(map.valueType);
      return map;
    }

    @Override
    public TList readListBegin() throws TException {
      TList list = super.readListBegin();
      checkElemType(list.elemType);
      return list;
    }

    @Override
    public TSet readSetBegin() throws TException {
      TSet set = super.readSetBegin();
      checkElemType(set.elemType);
      return set;
    }
  };

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
