package com.twitter.elephantbird.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

import com.twitter.elephantbird.thrift.test.AddressBook;
import com.twitter.elephantbird.thrift.test.MapStruct;
import com.twitter.elephantbird.thrift.test.Name;
import com.twitter.elephantbird.thrift.test.Person;
import com.twitter.elephantbird.thrift.test.PhoneNumber;
import com.twitter.elephantbird.thrift.test.PhoneType;
import com.twitter.elephantbird.thrift.test.PrimitiveListsStruct;
import com.twitter.elephantbird.thrift.test.PrimitiveSetsStruct;
import com.twitter.elephantbird.util.Protobufs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestThriftToDynamicProto {

  @Rule
  public ExpectedException exception = ExpectedException.none();

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

    List<String> strings = Lists.newLinkedList();
    strings.add("string1");
    strings.add("string2");
    strings.add("string3");

    List<Long> longs = Lists.newLinkedList();
    longs.add(1L);
    longs.add(2L);
    longs.add(3L);

    return listStruct.setStrings(strings).setLongs(longs);
  }

  private PrimitiveSetsStruct genPrimitiveSetsStruct() {
    PrimitiveSetsStruct setsStruct = new PrimitiveSetsStruct();

    Set<String> strings = Sets.newHashSet();
    strings.add("string1");
    strings.add("string2");
    strings.add("string3");

    Set<Long> longs = Sets.newHashSet();
    longs.add(1L);
    longs.add(2L);
    longs.add(3L);

    return setsStruct.setStrings(strings).setLongs(longs);
  }

  private MapStruct genMapStruct() {
    Map<Integer, String> map = Maps.newHashMap();
    map.put(1, "one");
    map.put(2, "two");
    map.put(3, "three");
    return new MapStruct(map);
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

  private void comparePrimitiveListStruct(PrimitiveListsStruct expected, Message actual) {
    List<String> strings = (List<String>) Protobufs.getFieldByName(actual, "strings");
    List<Long> longs = (List<Long>) Protobufs.getFieldByName(actual, "longs");
    assertEquals(expected.getStrings(), strings);
    assertEquals(expected.getLongs(), longs);
  }

  // We iterate through the Message and confirm its List elements match the Set elements
  private void comparePrimitiveSetsStruct(PrimitiveSetsStruct expected, Message actual) {
    List<String> actualStrings = (List<String>) Protobufs.getFieldByName(actual, "strings");
    List<Long> actualLongs = (List<Long>) Protobufs.getFieldByName(actual, "longs");

    Set<String> expectedStrings = expected.strings;
    Set<Long> expectedLongs = expected.longs;

    for (String entry : actualStrings) {
      assertTrue(expectedStrings.remove(entry));
    }
    assertEquals(expectedStrings.size(), 0);

    for (Long entry : actualLongs) {
      assertTrue(expectedLongs.remove(entry));
    }
    assertEquals(expectedLongs.size(), 0);
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
  public void testNestedStructsWhenDisabled() throws DescriptorValidationException {
    ThriftToDynamicProto<Person> thriftToProto =
      new ThriftToDynamicProto<Person>(Person.class);
    Person person = genPerson();
    Message msg = thriftToProto.convert(person);
    assertEquals(person.id, Protobufs.getFieldByName(msg, "id"));
    assertEquals(person.email, Protobufs.getFieldByName(msg, "email"));
    // nested structs not converted
    assertTrue(!Protobufs.hasFieldByName(msg, "name"));
    assertTrue(!Protobufs.hasFieldByName(msg, "phones"));
  }

  @Test
  public void testPrimitiveListConversionWhenNestedStructsEnabled() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveListsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveListsStruct>(PrimitiveListsStruct.class, true, false);
    PrimitiveListsStruct listStruct = genPrimitiveListsStruct();
    Message msg = thriftToProto.convert(listStruct);
    comparePrimitiveListStruct(listStruct, msg);
  }

  @Test
  public void testPrimitiveListConversionWhenNestedStructsDisabled() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveListsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveListsStruct>(PrimitiveListsStruct.class);
    PrimitiveListsStruct listStruct = genPrimitiveListsStruct();
    Message msg = thriftToProto.convert(listStruct);
    comparePrimitiveListStruct(listStruct, msg);
  }

  @Test
  public void testPrimitiveSetConversionWhenNestedStructsDisabled() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveSetsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveSetsStruct>(PrimitiveSetsStruct.class);
    PrimitiveSetsStruct setsStruct = genPrimitiveSetsStruct();
    Message msg = thriftToProto.convert(setsStruct);
    comparePrimitiveSetsStruct(setsStruct, msg);
  }

  @Test
  public void testPrimitiveSetConversionWhenNestedStructsEnabled() throws DescriptorValidationException {
    ThriftToDynamicProto<PrimitiveSetsStruct> thriftToProto =
      new ThriftToDynamicProto<PrimitiveSetsStruct>(PrimitiveSetsStruct.class, true, false);
    PrimitiveSetsStruct setsStruct = genPrimitiveSetsStruct();
    Message msg = thriftToProto.convert(setsStruct);
    comparePrimitiveSetsStruct(setsStruct, msg);
  }

  @Test
  public void testMapConversionWhenNestedStructsEnabled() throws DescriptorValidationException {
    ThriftToDynamicProto<MapStruct> thriftToProto =
      new ThriftToDynamicProto<MapStruct>(MapStruct.class, true, false);
    MapStruct mapStruct = genMapStruct();
    Message msg = thriftToProto.convert(mapStruct);

    Map<Integer, String> expected = mapStruct.entries;
    List<?> entries = (List)Protobufs.getFieldByName(msg, "entries");
    Set<?> expectedKeys = Sets.newHashSet(expected.keySet());
    for (Object entry : entries) {
      Message entryMsg = (Message) entry;
      Object key = Protobufs.getFieldByName(entryMsg, "key");
      assertTrue(expectedKeys.remove(key));

      Object value = Protobufs.getFieldByName(entryMsg, "value");
      assertEquals(expected.get(key), value);
    }

    assertEquals(0, expectedKeys.size());
  }

  @Test
  public void testMapConversionWhenNestedStructsDisabled() throws DescriptorValidationException {
    ThriftToDynamicProto<MapStruct> thriftToProto =
      new ThriftToDynamicProto<MapStruct>(MapStruct.class);
    MapStruct mapStruct = genMapStruct();
    Message msg = thriftToProto.convert(mapStruct);
    assertTrue(!Protobufs.hasFieldByName(msg, "entries"));
  }

  @Test
  public void testBadThriftTypeForGetFieldDescriptor() throws DescriptorValidationException {
    ThriftToDynamicProto<PhoneNumber> converter = new ThriftToDynamicProto<PhoneNumber>(PhoneNumber.class);

    exception.expect(IllegalStateException.class);
    converter.getFieldDescriptor(Person.class, "some_field");
  }

  @Test
  public void testGetFieldTypeDescriptor() throws DescriptorValidationException {
    ThriftToDynamicProto<Person> converter = new ThriftToDynamicProto<Person>(Person.class);
    Person person = genPerson();
    Message msg = converter.convert(person);

    FieldDescriptor expectedFd = msg.getDescriptorForType().findFieldByName("email");
    FieldDescriptor actualFd = converter.getFieldDescriptor(Person.class, "email");

    assertEquals(expectedFd, actualFd);
  }

  @Test
  public void testGetFileDescriptor() throws DescriptorValidationException {
    ThriftToDynamicProto<Person> converter = new ThriftToDynamicProto<Person>(Person.class);
    Person person = genPerson();
    Message msg = converter.convert(person);

    FileDescriptor expectedFd = msg.getDescriptorForType().getFile();
    FileDescriptor actualFd = converter.getFileDescriptor();

    assertEquals(expectedFd, actualFd);
  }
}
