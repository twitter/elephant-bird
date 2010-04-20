package com.twitter.elephantbird.pig.piggybank;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

public class ThriftToPigProtocol extends TProtocol {

  private static BagFactory bagFactory_ = BagFactory.getInstance();
  private static TupleFactory tupleFactory_  = TupleFactory.getInstance();


  private Stack<PigContainer> containerStack_ = new Stack<PigContainer>();
  private final TupleWrap BASE_TUPLE = new TupleWrap();
  private PigContainer currContainer_ = BASE_TUPLE;

  private int numOpenStructs_ = 0;

  // We want something that provides a generic interface for populating
  // Pig Tuples, Bags, and Maps. This does the trick.
  
  // TODO: factor this out into a general util to assist with conversions from Avro
  // and other formats.
  
  private abstract class PigContainer {
    public abstract Object getContents();
    public abstract void add(Object o) throws TException;
  }

  private class TupleWrap extends PigContainer {

    private final Tuple t;
    private int curIdx = 0;

    public TupleWrap() {
      t = tupleFactory_.newTuple();
    }

    public TupleWrap(int size) {
      t = tupleFactory_.newTuple(size);
    }

    public void reset() {
      curIdx = 0;
    }

    public Object getContents() { return t; }
    
    public void add(Object o) throws TException {
      if (curIdx == t.size()) {
        t.append(o);
        curIdx += 1;
      } else {
        try {
          t.set(curIdx++, o);
        } catch (ExecException e) {
          throw new TException(e);
        }
      }
    }
  }

  private class BagWrap extends PigContainer {

    private final DataBag b;

    public BagWrap() {
      b = bagFactory_.newDefaultBag();
    }

    @Override
    public void add(Object o) throws TException {
      try {
        b.add((Tuple) o);
      } catch (ClassCastException e) {
        throw new TException(e);
      }
    }

    @Override
    public Object getContents() {
      return b;
    }
  }

  private class MapWrap extends PigContainer {
    private final Map<String, Object> map;
    String currKey = null;

    public MapWrap() {
      map = new HashMap<String, Object>();
    }

    public MapWrap(int size) {
      map = new HashMap<String, Object>(size);
    }

    @Override
    public void add(Object o) throws TException {
      //we alternate between String keys and (converted) DataByteArray values.
      if (currKey == null) {
        try {
          currKey = (String) o;
        } catch (ClassCastException e) {
          throw new TException("Only String keys are allowed in maps.");
        }
      } else {
        map.put(currKey, o);
      }
    }

    @Override
    public Object getContents() {
      return map;
    }
  }


  private void pushContainer(PigContainer c) {
    containerStack_.add(c);
    currContainer_ = c; 
  }

  private PigContainer popContainer() throws TException {
    PigContainer c = containerStack_.pop();
    currContainer_ = containerStack_.peek();
    return c;
  }

  /**
   * Factory
   */
  public static class Factory implements TProtocolFactory {
    public TProtocol getProtocol(TTransport trans) {
      return new ThriftToPigProtocol(trans);
    }
  } 

  public ThriftToPigProtocol(TTransport trans) {
    super(trans);
  } 

  @Override
  public void writeBinary(byte[] bin) throws TException {
    currContainer_.add(new DataByteArray(bin));
  }

  @Override
  public void writeBool(boolean b) throws TException {
    currContainer_.add(Integer.valueOf(b ? 1 : 0));
  }

  @Override
  public void writeByte(byte b) throws TException {
    currContainer_.add(Integer.valueOf(b));
  }

  @Override
  public void writeDouble(double dub) throws TException {
    currContainer_.add(Double.valueOf(dub));
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
  }

  @Override
  public void writeFieldEnd() throws TException {    
  }

  @Override
  public void writeFieldStop() throws TException {
  }

  @Override
  public void writeI16(short i16) throws TException {
    currContainer_.add(Integer.valueOf(i16));
  }

  @Override
  public void writeI32(int i32) throws TException {
    currContainer_.add(i32);
  }

  @Override
  public void writeI64(long i64) throws TException {
    currContainer_.add(i64);
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    pushContainer(new TupleWrap(list.size));
  }

  @Override
  public void writeListEnd() throws TException {
    PigContainer c = popContainer();
    currContainer_.add(c.getContents());
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    pushContainer(new MapWrap(map.size));
  }

  @Override
  public void writeMapEnd() throws TException {
    PigContainer c = popContainer();
    currContainer_.add(c.getContents());
  }

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
  }

  /**
   * Normally this writes to the byte[] in the transport
   * but we don't want to do this -- the result of serialize() is to be discarded,
   * and instead getPigTuple() should be called after serializing.
   */
  @Override
  public void writeMessageEnd() throws TException {
  }

  public Tuple getPigTuple() {
    return (Tuple) BASE_TUPLE.getContents();
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    pushContainer(new BagWrap());
  }

  @Override
  public void writeSetEnd() throws TException {
    PigContainer c = popContainer();
    currContainer_.add(c.getContents());
  }

  @Override
  public void writeString(String str) throws TException {
    currContainer_.add(str);
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    if (numOpenStructs_ > 0) {
      pushContainer(new TupleWrap());
    } else {
      containerStack_.clear();
      ((Tuple)BASE_TUPLE.getContents()).reference(tupleFactory_.newTuple());
      BASE_TUPLE.reset();
      pushContainer(BASE_TUPLE);
    }
    numOpenStructs_ += 1;
  }

  @Override
  public void writeStructEnd() throws TException {
    if (numOpenStructs_ > 1) {
      PigContainer c = popContainer();
      currContainer_.add(c.getContents());
    }
    numOpenStructs_ -= 1;
  }

  @Override
  public byte[] readBinary() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean readBool() throws TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte readByte() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double readDouble() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TField readFieldBegin() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readFieldEnd() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public short readI16() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int readI32() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long readI64() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TList readListBegin() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readListEnd() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public TMap readMapBegin() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readMapEnd() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public TMessage readMessageBegin() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readMessageEnd() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public TSet readSetBegin() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readSetEnd() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public String readString() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TStruct readStructBegin() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readStructEnd() throws TException {
    // TODO Auto-generated method stub

  }
}
