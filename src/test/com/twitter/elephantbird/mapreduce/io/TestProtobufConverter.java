package com.twitter.elephantbird.mapreduce.io;


import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.elephantbird.pig.piggybank.Fixtures;
import com.twitter.elephantbird.pig.util.ProtobufToPig;

public class TestProtobufConverter {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testFromBytes() {
    ProtobufConverter<Person> personConverter1 = ProtobufConverter.newInstance(
        Person.class, Fixtures.buildExtensionRegistry());
    assertEquals(Fixtures.buildPersonProto(true),
        personConverter1.fromBytes(personConverter1.toBytes(
            Fixtures.buildPersonProto(true))));

    ProtobufConverter<Person> personConverter2 = ProtobufConverter.newInstance(
        Person.class);
    assertEquals(Fixtures.buildPersonProto(false),
        personConverter2.fromBytes(personConverter2.toBytes(
            Fixtures.buildPersonProto(false))));

    ProtobufConverter<AddressBook> abConverter1 = ProtobufConverter.newInstance(
        AddressBook.class, Fixtures.buildExtensionRegistry());
    assertEquals(Fixtures.buildAddressBookProto(true),
        abConverter1.fromBytes(abConverter1.toBytes(
            Fixtures.buildAddressBookProto(true))));

    ProtobufConverter<AddressBook> abConverter2 = ProtobufConverter.newInstance(
        AddressBook.class);
    assertEquals(Fixtures.buildAddressBookProto(false),
        abConverter2.fromBytes(abConverter2.toBytes(
            Fixtures.buildAddressBookProto(false))));
  }
}
