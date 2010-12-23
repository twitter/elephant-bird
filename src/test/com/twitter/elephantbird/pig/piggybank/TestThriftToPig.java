package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.test.Fixtures;
import org.junit.Test;

import thrift.test.HolyMoley;
import thrift.test.Nesting;
import thrift.test.OneOfEach;

import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.util.TypeRef;

public class TestThriftToPig {

  static <M extends TBase<?>> Tuple toTuple(M obj) throws TException {
    // it is very inefficient to create one ThriftToPig for each Thrift object,
    // but good enough for unit testing.
    return ThriftToPig.newInstance(new TypeRef<M>(obj.getClass()){}).getPigTuple(obj);
  }

  @Test
  public void testThriftToPig() throws TException, ExecException {
    OneOfEach ooe = Fixtures.oneOfEach;
    Nesting n = Fixtures.nesting;

    HolyMoley hm = Fixtures.holyMoley;

    assertEquals(
        "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"-"+ooe.zomg_unicode+"-0-base64-{(1),(2),(3)}-{(1),(2),(3)}-{(1),(2),(3)}",
        toTuple(ooe).toDelimitedString("-"));

    assertEquals("(31337,I am a bonk... xor!)-(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \","+n.my_ooe.zomg_unicode+",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})",
        toTuple(n).toDelimitedString("-"));

    assertEquals("{(1,0,34,27000,16777216,6000000000,3.141592653589793,JSON THIS! \"," + ooe.zomg_unicode +
        ",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)}),(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \"," +
        ooe.zomg_unicode + ",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})}-{({}),({(then a one, two),(three!),(FOUR!!)}),({(and a one),(and a two)})}-{zero={}, three={}, two={(1,Wait.),(2,What?)}}",
        (toTuple(hm).toDelimitedString("-")));

    // Test null fields :
    OneOfEach mostly_ooe = new OneOfEach(ooe);
    mostly_ooe.setBase64(null);
    mostly_ooe.setI16_list(null);
    assertEquals(
        "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"-"+ooe.zomg_unicode+"-0--{(1),(2),(3)}--{(1),(2),(3)}",
        toTuple(mostly_ooe).toDelimitedString("-"));

    Nesting n2 = new Nesting(n);
    n2.getMy_bonk().setMessage(null);
    n2.setMy_ooe(mostly_ooe);
    assertEquals("(31337,)-(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \","+n.my_ooe.zomg_unicode+",0,,{(1),(2),(3)},,{(1),(2),(3)})",
        toTuple(n2).toDelimitedString("-"));

    // test enum.
    PhoneNumber ph = new PhoneNumber();
    ph.setNumber("415-555-5555");
    ph.setType(PhoneType.HOME);
    assertEquals("415-555-5555,HOME", toTuple(ph).toDelimitedString(","));

  }
}
