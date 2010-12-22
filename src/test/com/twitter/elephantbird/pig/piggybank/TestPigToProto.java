package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.google.protobuf.Message;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.pig8.util.PigToProtobuf;

public class TestPigToProto {
  
  @Test
  public void testPigToProto() throws ExecException {
    Tuple abTuple = Fixtures.buildAddressBookTuple();
    PigToProtobuf protoConverter = new PigToProtobuf();
    Message proto = protoConverter.tupleToMessage(AddressBook.newBuilder(), abTuple);
    assertEquals(Fixtures.buildAddressBookProto(), proto);
  }
}
