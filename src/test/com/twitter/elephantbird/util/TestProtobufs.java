package com.twitter.elephantbird.util;

import com.google.protobuf.Message;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestProtobufs {

  @Test
  public void testGetInnerProtobufClass() {
    String canonicalClassName = "com.twitter.data.proto.tutorial.AddressBookProtos.Person";
    Class<? extends Message> klass = Protobufs.getInnerProtobufClass(canonicalClassName);
    assertEquals(klass, Person.class);
  }
}
