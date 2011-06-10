package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.TypeRef;

/**
 * The base class for a Pig UDF that takes as input a tuple containing a single element, the
 * bytes of a serialized Thrift object as a DataByteArray.  It outputs the Thrift object in
 * expanded form.  The specific Thrift class is supplied through an argument to the
 * UDF constructor as in :
 * <pre>
 *   DEFINE PersonThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('com.twitter.elephantbird.thrift.Person');
 *   persons = FOREACH thriftobjects GENERATE PersonThriftBytesToTuple($0);
 * </pre>
 */

public class ThriftBytesToTuple<M extends TBase<?,?>> extends EvalFunc<Tuple> {

  private final TypeRef<M> typeRef;
  private final ThriftConverter<M> thriftConverter;
  private final ThriftToPig<M> thriftToPig;

  public ThriftBytesToTuple(String thriftClassName) {
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    thriftConverter = ThriftConverter.newInstance(typeRef);
    thriftToPig = ThriftToPig.newInstance(typeRef);
  }

  @Override
  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() < 1) return null;
    try {
      DataByteArray bytes = (DataByteArray) input.get(0);
      M value = thriftConverter.fromBytes(bytes.get());
      return value == null ? null : thriftToPig.getPigTuple(value);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    return ThriftToPig.toSchema(typeRef.getRawClass());
  }
}
