package com.twitter.elephantbird.pig.piggybank;

import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TMemoryBuffer;

@SuppressWarnings("unchecked")
public class ThriftToTuple<T extends TBase> {
  
  private final ThriftToPigProtocol toPigProtocol_ = new ThriftToPigProtocol(new TMemoryBuffer(1));
  public Tuple convert(T thriftObj) throws TException {
    thriftObj.write(toPigProtocol_);
    return toPigProtocol_.getPigTuple();
  }
}
