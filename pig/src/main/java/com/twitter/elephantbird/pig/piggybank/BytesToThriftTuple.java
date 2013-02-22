package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;

import com.twitter.elephantbird.thrift.ThriftBinaryDeserializer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is an abstract UDF for converting serialized Thrift objects into Pig tuples.
 * To create a converter for your Thrift class <code>MyThriftClass</code>, you simply need to extend
 * <code>BytesToThriftTuple</code> with something like this:
 *<pre>
 * {@code
 * public class BytesToSimpleLocation extends BytesToThriftTuple<MyThriftClass> {
 *
 *   public BytesToSimpleLocation() {
 *     setTypeRef(new TypeRef<MyThriftClass>() {});
 *   }
 * }}
 *</pre>
 */
public abstract class BytesToThriftTuple<T extends TBase<?, ?>> extends EvalFunc<Tuple> {

  private final TDeserializer deserializer_ = new ThriftBinaryDeserializer();
  private ThriftToPig<T> thriftToTuple_;
  private TypeRef<T> typeRef_;

  /**
   * Set the type parameter so it doesn't get erased by Java.  Must be called by the constructor!
   *
   * @param typeRef
   */
  public void setTypeRef(TypeRef<T> typeRef) {
    typeRef_ = typeRef;
    thriftToTuple_ = ThriftToPig.newInstance(typeRef);
  }


  @Override
  public Tuple exec(org.apache.pig.data.Tuple input) throws IOException {
    if (input == null || input.size() < 1) {
      return null;
    }
    try {
      T tObj = typeRef_.safeNewInstance();
      DataByteArray dbarr = (DataByteArray) input.get(0);
      deserializer_.deserialize(tObj, dbarr.get());
      return thriftToTuple_.getPigTuple(tObj);
    } catch (IOException e) {
      log.warn("Caught exception "+e.getMessage());
      return null;
    } catch (TException e) {
      log.warn("Unable to deserialize Thrift object: "+e);
      return null;
    }
  }
}
