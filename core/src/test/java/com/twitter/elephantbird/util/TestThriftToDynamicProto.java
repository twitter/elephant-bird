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
import com.twitter.elephantbird.util.Protobufs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class TestThriftToDynamicProto {
  private PhoneNumber mkPhoneNumber(String number, PhoneType type) {
    PhoneNumber phoneNumber = new PhoneNumber(number);
    phoneNumber.setType(type);
    return phoneNumber;
  }

  private List<PhoneNumber> genPhoneList() {
    List<PhoneNumber> phoneList = Lists.newLinkedList();
    phoneList.add(mkPhoneNumber("123", PhoneType.HOME));
    phoneList.add(mkPhoneNumber("456", PhoneType.MOBILE));
    phoneList.add(mkPhoneNumber("789", PhoneType.WORK));
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

  @Test
  public void testStructConversion() throws DescriptorValidationException {
    PhoneNumber thriftPhone = new PhoneNumber("123-34-5467");
    thriftPhone.type = PhoneType.HOME;
    ThriftToDynamicProto<PhoneNumber> thriftToProto = new ThriftToDynamicProto<PhoneNumber>(PhoneNumber.class);
    Message msg = thriftToProto.convert(thriftPhone);
    assertEquals(thriftPhone.number, Protobufs.getFieldByName(msg, "number"));
    assertEquals(thriftPhone.type.toString(), Protobufs.getFieldByName(msg, "type"));
  }

  @Test
  public void testPrimitiveListConversion() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveListsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveListsStruct>(PrimitiveListsStruct.class);
    PrimitiveListsStruct listStruct = genPrimitiveListsStruct();
    Message msg = thriftToProto.convert(listStruct);

    List<String> strings = (List<String>) Protobufs.getFieldByName(msg, "strings");
    List<Long> longs = (List<Long>) Protobufs.getFieldByName(msg, "longs");

    assertEquals(listStruct.getStrings(), strings);
    assertEquals(listStruct.getLongs(), longs);
  }
}
