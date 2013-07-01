package com.twitter.elephantbird.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;

/**
 * Functionally exactly same as {@link TBinaryProtocol}. <p>
 *
 * Overwrites a few methods so that some malformed messages don't end up
 * taking excessively large amounts of cpu inside TProtocolUtil.skip().
 */
public class ThriftBinaryProtocol extends TBinaryProtocol {

  public ThriftBinaryProtocol(TTransport trans) {
    super(trans);
  }

  /**
   * Ensures that an element type in a for container (List, Set, Map) is
   * a valid container.
   *
   * @param type
   * @throws TException if the type is not one of the expected type.
   */
  public static void checkContainerElemType(byte type) throws TException {
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
    checkContainerElemType(map.keyType);
    checkContainerElemType(map.valueType);
    return map;
  }

  @Override
  public TList readListBegin() throws TException {
    TList list = super.readListBegin();
    checkContainerElemType(list.elemType);
    return list;
  }

  @Override
  public TSet readSetBegin() throws TException {
    TSet set = super.readSetBegin();
    checkContainerElemType(set.elemType);
    return set;
  }

  public static class Factory implements TProtocolFactory {

    public TProtocol getProtocol(TTransport trans) {
      return new ThriftBinaryProtocol(trans);
    }
  }
}
