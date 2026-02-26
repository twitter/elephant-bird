package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;
import org.junit.Test;

import com.google.protobuf.Message;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.examples.proto.ThriftFixtures.OneOfEach;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.ThriftToProto;


public class TestPigToProto {

  @Test
  public void testPigToProto() throws ExecException, TException {
    Tuple abTuple = Fixtures.buildAddressBookTuple();
    Message proto = PigToProtobuf.tupleToMessage(AddressBook.newBuilder(), abTuple);
    assertEquals(Fixtures.buildAddressBookProto(), proto);

    // test with OneOfEach.
    thrift.test.OneOfEach thrift_ooe = org.apache.thrift.Fixtures.oneOfEach;
    OneOfEach proto_ooe = ThriftToProto.newInstance(thrift_ooe, OneOfEach.newBuilder().build()).convert(thrift_ooe);
    //tuple from Thrift ooe :
    Tuple tuple_ooe = ThriftToPig.newInstance(thrift.test.OneOfEach.class).getPigTuple(thrift_ooe);

    assertEquals(proto_ooe, PigToProtobuf.tupleToMessage(OneOfEach.class, tuple_ooe));

  }
}
