package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.Fixtures;
import org.junit.Test;

import thrift.test.HolyMoley;
import thrift.test.Nesting;
import thrift.test.OneOfEach;

import com.google.common.collect.Lists;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.util.TypeRef;

public class TestThriftToPig {

  private static final TupleFactory tupleFactory  = TupleFactory.getInstance();

  private static enum TestType {
    THRIFT_TO_PIG,
    BYTES_TO_TUPLE,
    TUPLE_TO_THRIFT,
  }

  static <M extends TBase<?, ?>> Tuple thriftToPig(M obj) throws TException {
    // it is very inefficient to create one ThriftToPig for each Thrift object,
    // but good enough for unit testing.
    return ThriftToPig.newInstance(new TypeRef<M>(obj.getClass()){}).getPigTuple(obj);
  }

  static <M extends TBase<?, ?>> Tuple thriftToLazyTuple(M obj) throws TException {
    return ThriftToPig.newInstance(new TypeRef<M>(obj.getClass()){}).getLazyTuple(obj);
  }

  static <M extends TBase<?, ?>> Tuple bytesToTuple(M obj)
                                throws TException, ExecException, IOException {
    // test ThriftBytesToTuple UDF.
    // first serialize obj and then invoke the UDF.
    TypeRef<M> typeRef = new TypeRef<M>(obj.getClass()){};
    ThriftConverter<M> converter = ThriftConverter.newInstance(typeRef);
    ThriftBytesToTuple<M> tTuple = new ThriftBytesToTuple<M>(obj.getClass().getName());
    Tuple tuple = tupleFactory.newTuple(1);
    tuple.set(0, new DataByteArray(converter.toBytes(obj)));
    return tTuple.exec(tuple);
  }

  static <M extends TBase<?, ?>> Tuple pigToThrift(M obj) throws TException {
    // Test PigToThrift using the tuple returned by thriftToPig.
    // also use LazyTuple
    Tuple t = thriftToLazyTuple(obj);
    PigToThrift<M> p2t = PigToThrift.newInstance(new TypeRef<M>(obj.getClass()){});
    assertEquals(obj, p2t.getThriftObject(t));
    return t;
  }

  static <M extends TBase<?, ?>> Tuple toTuple(TestType type, M obj) throws Exception {
    switch (type) {
    case THRIFT_TO_PIG:
      return thriftToPig(obj);
    case BYTES_TO_TUPLE:
      return bytesToTuple(obj);
    case TUPLE_TO_THRIFT:
      return pigToThrift(obj);
    default:
      return null;
    }
  }

  @Test
  public void test() throws Exception {
    tupleTest(TestType.THRIFT_TO_PIG);
    tupleTest(TestType.BYTES_TO_TUPLE);
    tupleTest(TestType.TUPLE_TO_THRIFT);
  }

  private void tupleTest(TestType type) throws Exception {
    OneOfEach ooe = Fixtures.oneOfEach;
    Nesting n = Fixtures.nesting;

    ThriftConverter<HolyMoley> hmConverter = ThriftConverter.newInstance(HolyMoley.class);
    // use a deserialized hm object so that hm.contains HashSet iteration is a bit more predictable
    HolyMoley hm = hmConverter.fromBytes(hmConverter.toBytes(Fixtures.holyMoley));

    assertEquals(
        "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"-"+ooe.zomg_unicode+"-0-base64-{(1),(2),(3)}-{(1),(2),(3)}-{(1),(2),(3)}",
        toTuple(type, ooe).toDelimitedString("-"));

    assertEquals("(31337,I am a bonk... xor!)-(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \","+n.my_ooe.zomg_unicode+",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})",
        toTuple(type, n).toDelimitedString("-"));

    assertEquals("{(1,0,34,27000,16777216,6000000000,3.141592653589793,JSON THIS! \"," + ooe.zomg_unicode +
        ",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)}),(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \"," +
        ooe.zomg_unicode + ",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})}-{({}),({(and a one),(and a two)}),({(then a one, two),(three!),(FOUR!!)})}-{zero={}, three={}, two={(1,Wait.),(2,What?)}}",
        (toTuple(type, hm).toDelimitedString("-")));

    // Test null fields. Pick the fields that have defaults of null
    // so that extra round of seralization and deserialization does not affect it.
    OneOfEach mostly_ooe = new OneOfEach(ooe);
    mostly_ooe.setBase64((ByteBuffer)null);
    mostly_ooe.setZomg_unicode(null);
    assertEquals(
        "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"--0--{(1),(2),(3)}-{(1),(2),(3)}-{(1),(2),(3)}",
        toTuple(type, mostly_ooe).toDelimitedString("-"));

    Nesting n2 = new Nesting(n);
    n2.getMy_bonk().setMessage(null);
    n2.setMy_ooe(mostly_ooe);
    assertEquals("(31337,)-(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \",,0,,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})",
        toTuple(type, n2).toDelimitedString("-"));

    // test enum.
    PhoneNumber ph = new PhoneNumber();
    ph.setNumber("415-555-5555");
    ph.setType(PhoneType.HOME);
    assertEquals("415-555-5555,HOME", toTuple(type, ph).toDelimitedString(","));

    Person person = new Person(new Name("bob", "jenkins"), 42, "foo@bar.com", Lists.newArrayList(ph));
    assertEquals("(bob,jenkins),42,foo@bar.com,{(415-555-5555,HOME)}", toTuple(type, person).toDelimitedString(","));
  }
}
