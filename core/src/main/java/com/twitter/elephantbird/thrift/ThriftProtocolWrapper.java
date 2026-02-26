package com.twitter.elephantbird.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

import static com.twitter.elephantbird.thrift.ThriftBinaryProtocol.checkContainerElemType;

/**
 * A wrapper for TProtocol. <p>
 *
 * Sanity checks container element types (list, set, map) read by the wrapped
 * TProtocol so that some malformed messages don't end up taking excessively
 * large amounts of cpu inside TProtocolUtil.skip(). <p>
 */
public class ThriftProtocolWrapper extends TProtocol {

  protected final TProtocol wrapped;

  public ThriftProtocolWrapper(TProtocol wrapped) {
    super(wrapped.getTransport());
    this.wrapped = wrapped;
  }

  @Override
  public TTransport getTransport() {
    return wrapped.getTransport();
  }

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    wrapped.writeMessageBegin(message);
  }

  @Override
  public void writeMessageEnd() throws TException {
    wrapped.writeMessageEnd();
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    wrapped.writeStructBegin(struct);
  }

  @Override
  public void writeStructEnd() throws TException {
    wrapped.writeStructEnd();
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    wrapped.writeFieldBegin(field);
  }

  @Override
  public void writeFieldEnd() throws TException {
    wrapped.writeFieldEnd();
  }

  @Override
  public void writeFieldStop() throws TException {
    wrapped.writeFieldStop();
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    wrapped.writeMapBegin(map);
  }

  @Override
  public void writeMapEnd() throws TException {
    wrapped.writeMapEnd();
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    wrapped.writeListBegin(list);
  }

  @Override
  public void writeListEnd() throws TException {
    wrapped.writeListEnd();
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    wrapped.writeSetBegin(set);
  }

  @Override
  public void writeSetEnd() throws TException {
    wrapped.writeSetEnd();
  }

  @Override
  public void writeBool(boolean b) throws TException {
    wrapped.writeBool(b);
  }

  @Override
  public void writeByte(byte b) throws TException {
    wrapped.writeByte(b);
  }

  @Override
  public void writeI16(short i16) throws TException {
    wrapped.writeI16(i16);
  }

  @Override
  public void writeI32(int i32) throws TException {
    wrapped.writeI32(i32);
  }

  @Override
  public void writeI64(long i64) throws TException {
    wrapped.writeI64(i64);
  }

  @Override
  public void writeDouble(double dub) throws TException {
    wrapped.writeDouble(dub);
  }

  @Override
  public void writeString(String str) throws TException {
    wrapped.writeString(str);
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    wrapped.writeBinary(buf);
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    return wrapped.readMessageBegin();
  }

  @Override
  public void readMessageEnd() throws TException {
    wrapped.readMessageEnd();
  }

  @Override
  public TStruct readStructBegin() throws TException {
    return wrapped.readStructBegin();
  }

  @Override
  public void readStructEnd() throws TException {
    wrapped.readStructEnd();
  }

  @Override
  public TField readFieldBegin() throws TException {
    return wrapped.readFieldBegin();
  }

  @Override
  public void readFieldEnd() throws TException {
    wrapped.readFieldEnd();
  }

  @Override
  public TMap readMapBegin() throws TException {
    TMap map = wrapped.readMapBegin();
    checkContainerElemType(map.keyType);
    checkContainerElemType(map.valueType);
    return map;
  }

  @Override
  public void readMapEnd() throws TException {
    wrapped.readMapEnd();
  }

  @Override
  public TList readListBegin() throws TException {
    TList list = wrapped.readListBegin();
    checkContainerElemType(list.elemType);
    return list;
  }

  @Override
  public void readListEnd() throws TException {
    wrapped.readListEnd();
  }

  @Override
  public TSet readSetBegin() throws TException {
    TSet set = wrapped.readSetBegin();
    checkContainerElemType(set.elemType);
    return set;
  }

  @Override
  public void readSetEnd() throws TException {
    wrapped.readSetEnd();
  }

  @Override
  public boolean readBool() throws TException {
    return wrapped.readBool();
  }

  @Override
  public byte readByte() throws TException {
    return wrapped.readByte();
  }

  @Override
  public short readI16() throws TException {
    return wrapped.readI16();
  }

  @Override
  public int readI32() throws TException {
    return wrapped.readI32();
  }

  @Override
  public long readI64() throws TException {
    return wrapped.readI64();
  }

  @Override
  public double readDouble() throws TException {
    return wrapped.readDouble();
  }

  @Override
  public String readString() throws TException {
    return wrapped.readString();
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return wrapped.readBinary();
  }

  @Override
  public void reset() {
    wrapped.reset();
  }
}
