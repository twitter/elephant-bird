package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.pig.piggybank.AddressBookProtobufBytesToTuple;

public class TestProtoToPig {

  private static TupleFactory tf_ = TupleFactory.getInstance();

  @Test
  public void testProtoToPig() throws IOException {
    AddressBook abProto = Fixtures.buildAddressBookProto();

    Tuple abProtoTuple = tf_.newTuple(new DataByteArray(abProto.toByteArray()));

    AddressBookProtobufBytesToTuple abProtoToPig = new AddressBookProtobufBytesToTuple();
    Tuple abTuple = abProtoToPig.exec(abProtoTuple);
    assertEquals("{(Elephant Bird,123,elephant@bird.com,{(415-999-9999,HOME),(415-666-6666,MOBILE),(415-333-3333,WORK)})}",
        abTuple.toDelimitedString(","));  
  }
}
