package com.twitter.elephantbird.util;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Message;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.data.proto.tutorial.thrift.PrimitiveListsStruct;
import com.twitter.data.proto.tutorial.thrift.PrimitiveSetsStruct;
import com.twitter.elephantbird.util.Protobufs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestThriftToDynamicProto {
  private PhoneNumber genPhoneNumber(String number, PhoneType type) {
    PhoneNumber phoneNumber = new PhoneNumber(number);
    phoneNumber.setType(type);
    return phoneNumber;
  }

  private List<PhoneNumber> genPhoneList() {
    List<PhoneNumber> phoneList = Lists.newLinkedList();
    phoneList.add(genPhoneNumber("123", PhoneType.HOME));
    phoneList.add(genPhoneNumber("456", PhoneType.MOBILE));
    phoneList.add(genPhoneNumber("789", PhoneType.WORK));
    return phoneList;
  }

  private Person genPerson() {
    Person person = new Person();
    person.setName(new Name("Johnny", "Appleseed"));
    person.setId(123);
    person.setEmail("email@address.com");
    person.setPhones(genPhoneList());
    return person;
  }

  private PrimitiveListsStruct genPrimitiveListsStruct() {
    PrimitiveListsStruct listStruct = new PrimitiveListsStruct();

    List strings = Lists.newLinkedList();
    strings.add("string1");
    strings.add("string2");
    strings.add("string3");

    List longs = Lists.newLinkedList();
    longs.add(1L);
    longs.add(2L);
    longs.add(3L);

    return listStruct.setStrings(strings).setLongs(longs);
  }

  private void compareName(Name expected, Message actual) {
    assertNotNull(actual);
    assertEquals(expected.first_name, Protobufs.getFieldByName(actual, "first_name"));
    assertEquals(expected.last_name,  Protobufs.getFieldByName(actual, "last_name"));
  }

  private void comparePhoneNumbers(List<PhoneNumber> expected, List<?> actual) {
    assertNotNull(actual);
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      PhoneNumber number = expected.get(i);
      Message msg = (Message) actual.get(i);
      assertEquals(number.number, Protobufs.getFieldByName(msg, "number"));
      assertEquals(number.type.toString(), Protobufs.getFieldByName(msg, "type"));
    }
  }

  @Test
  public void testSimpleStructConversion() throws DescriptorValidationException {
    PhoneNumber thriftPhone = genPhoneNumber("123-34-5467", PhoneType.HOME);
    ThriftToDynamicProto<PhoneNumber> thriftToProto = new ThriftToDynamicProto<PhoneNumber>(PhoneNumber.class);
    Message msg = thriftToProto.convert(thriftPhone);
    assertEquals(thriftPhone.number, Protobufs.getFieldByName(msg, "number"));
    assertEquals(thriftPhone.type.toString(), Protobufs.getFieldByName(msg, "type"));
  }

  @Test
  public void testPrimitiveListConversionWhenNestedStructsDisabled() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveListsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveListsStruct>(PrimitiveListsStruct.class);
    PrimitiveListsStruct listStruct = genPrimitiveListsStruct();
    Message msg = thriftToProto.convert(listStruct);
    List<String> strings = (List<String>) Protobufs.getFieldByName(msg, "strings");
    List<Long> longs = (List<Long>) Protobufs.getFieldByName(msg, "longs");
    assertEquals(listStruct.getStrings(), strings);
    assertEquals(listStruct.getLongs(), longs);
  }

  @Test
  public void testPrimitiveListConversionWhenNestedStructsEnabled() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveListsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveListsStruct>(PrimitiveListsStruct.class, true, false);
    PrimitiveListsStruct listStruct = genPrimitiveListsStruct();
    Message msg = thriftToProto.convert(listStruct);
    List<String> strings = (List<String>) Protobufs.getFieldByName(msg, "strings");
    List<Long> longs = (List<Long>) Protobufs.getFieldByName(msg, "longs");
    assertEquals(listStruct.getStrings(), strings);
    assertEquals(listStruct.getLongs(), longs);
  }

  @Test
  public void testNestedStructsWhenDisabled() throws DescriptorValidationException {
    ThriftToDynamicProto<Person> thriftToProto =
      new ThriftToDynamicProto<Person>(Person.class);
    Person person = genPerson();
    Message msg = thriftToProto.convert(person);
    assertEquals(person.id, Protobufs.getFieldByName(msg, "id"));
    assertEquals(person.email, Protobufs.getFieldByName(msg, "email"));
    assertTrue(!Protobufs.hasFieldByName(msg, "name"));
    assertTrue(!Protobufs.hasFieldByName(msg, "phones"));
  }

  @Test
  public void testNestedStructsWhenEnabled() throws DescriptorValidationException {
    ThriftToDynamicProto<Person> thriftToProto =
      new ThriftToDynamicProto<Person>(Person.class, true, false);
    Person person = genPerson();
    Message msg = thriftToProto.convert(person);
    assertEquals(person.id, Protobufs.getFieldByName(msg, "id"));
    assertEquals(person.email, Protobufs.getFieldByName(msg, "email"));
    compareName(person.name, (Message) Protobufs.getFieldByName(msg, "name"));
    comparePhoneNumbers(person.phones, (List) Protobufs.getFieldByName(msg, "phones"));
  }

  @Test
  public void testPrimitiveSetConversion() throws DescriptorValidationException {
  }
}
