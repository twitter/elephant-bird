package com.twitter.elephantbird.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.elephantbird.pig.piggybank.Fixtures;
import com.twitter.elephantbird.util.Protobufs;

public class TestProtobufs {

  private static final AddressBook ab_ = Fixtures.buildAddressBookProto();
  private static final byte[] abBytes_ = ab_.toByteArray();
  
  @Test
  public void testGetInnerProtobufClass() {
    String canonicalClassName = "com.twitter.data.proto.tutorial.AddressBookProtos.Person";
    Class<? extends Message> klass = Protobufs.getInnerProtobufClass(canonicalClassName);
    assertEquals(klass, Person.class);
  }
  
  @Test
  public void testDynamicParsing() {
    assertEquals(ab_, Protobufs.parseDynamicFrom(AddressBook.class, abBytes_));
  }
  
  @Test
  public void testStaticParsing() {
    assertEquals(ab_, Protobufs.parseFrom(AddressBook.class, abBytes_));
  }
  
  @Test 
  public void testConverterParsing() {
    Function<byte[], AddressBook> protoConverter = Protobufs.getProtoConverter(AddressBook.class);
    assertEquals(ab_, protoConverter.apply(abBytes_));
  }
}
