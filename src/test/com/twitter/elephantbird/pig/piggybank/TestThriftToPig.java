package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.thrift.TException;
import org.apache.thrift.test.Fixtures;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import thrift.test.HolyMoley;
import thrift.test.Nesting;
import thrift.test.OneOfEach;

public class TestThriftToPig {

  @Test
  public void testThriftToPig() throws TException, ExecException {
    OneOfEach ooe = Fixtures.oneOfEach;
    Nesting n = Fixtures.nesting;

    HolyMoley hm = Fixtures.holyMoley;

    TMemoryBuffer buffer = new TMemoryBuffer(1024);
    ThriftToPigProtocol proto = new ThriftToPigProtocol(buffer);

    ooe.write(proto);
    assertEquals(
       "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"-"+ooe.zomg_unicode+"-0-base64-(1,2,3)-(1,2,3)-(1L,2L,3L)", 
        proto.getPigTuple().toDelimitedString("-"));

    n.write(proto);
    assertEquals("(31337,I am a bonk... xor!)-(1,0,35,27000,16777216,6000000000L,3.141592653589793,JSON THIS! \","+n.my_ooe.zomg_unicode+",0,base64,(1,2,3),(1,2,3),(1L,2L,3L))", 
        proto.getPigTuple().toDelimitedString("-"));

    hm.write(proto);
    assertEquals("((1,0,34,27000,16777216,6000000000L,3.141592653589793,JSON THIS! \"," + ooe.zomg_unicode + 
        ",0,base64,(1,2,3),(1,2,3),(1L,2L,3L)),(1,0,35,27000,16777216,6000000000L,3.141592653589793,JSON THIS! \"," +
        ooe.zomg_unicode + ",0,base64,(1,2,3),(1,2,3),(1L,2L,3L)))-{(),(then a one, two,three!,FOUR!!),(and a one,and a two)}-{two=()}", 
        (proto.getPigTuple().toDelimitedString("-")));

    assertTrue(true);
  }

}
