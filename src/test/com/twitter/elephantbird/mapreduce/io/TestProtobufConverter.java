package com.twitter.elephantbird.mapreduce.io;


import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.InvalidProtocolBufferException;
import com.twitter.data.proto.tutorial.AddressBookProtos;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.data.proto.tutorial.AddressBookProtos.PersonExt;

public class TestProtobufConverter {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testFromBytes() throws InvalidProtocolBufferException {
    Person p1 = Person.newBuilder()
    .setEmail("email1@example.com")
    .setId(74)
    .setName("Example Person")
    .addPhone(PhoneNumber.newBuilder().setType(PhoneType.MOBILE).setNumber("2930423").build())
    .addPhone(PhoneNumber.newBuilder().setType(PhoneType.HOME).setNumber("214121").build())
    .setExtension(PersonExt.extInfo, PersonExt.newBuilder().setAddress("Rd. Foo").build())
    .build();

//    for(Entry<FieldDescriptor, Object> e: p1.getAllFields().entrySet()) {
//      System.out.println(e.getKey().getName());
//    }

    ExtensionRegistry personExtregistry = ExtensionRegistry.newInstance();
    AddressBookProtos.registerAllExtensions(personExtregistry);

    ProtobufConverter<Person> personConverter1 = ProtobufConverter.newInstance(
        Person.class, personExtregistry);
    System.out.println("----------------------------");
    System.out.println(personConverter1.fromBytes(personConverter1.toBytes(p1)));
    System.out.println("***********************************");
    System.out.println(personConverter1.fromBytes(personConverter1.toBytes(p1)).getUnknownFields());
    System.out.println("----------------------------");
    Person.Builder builder = Person.newBuilder();
    builder.mergeFrom(p1.toByteArray());
    Person pp = personConverter1.fromBytes(p1.toByteArray());
    System.out.println(pp);
    System.out.println(pp.hasExtension(PersonExt.extInfo));
    System.out.println(pp.getUnknownFields().asMap().size());


//    System.out.println(builder.build());
    assertEquals(p1, personConverter1.fromBytes(personConverter1.toBytes(p1)));

    Person p2 = Person.newBuilder()
    .setId(7334)
    .setName("Another person")
    .addPhone(PhoneNumber.newBuilder().setType(PhoneType.MOBILE).setNumber("030303").build())
    .build();
    ProtobufConverter<Person> personConverter2 = ProtobufConverter.newInstance(
        Person.class);
    assertEquals(p2, personConverter2.fromBytes(personConverter2.toBytes(p2)));

    AddressBook ab1 = AddressBook.newBuilder()
    .addPerson(p1)
    .addPerson(p2)
    .setExtension(AddressBookProtos.name, "Private")
    .build();

    ExtensionRegistry abExtRegistry = ExtensionRegistry.newInstance();
    abExtRegistry.add(AddressBookProtos.name);
    ProtobufConverter<AddressBook> abConverter1 = ProtobufConverter.newInstance(
        AddressBook.class, abExtRegistry);

    try {
      System.out.println(PersonExt.class.getName());
      Class<?> cls = Class.forName("com.twitter.data.proto.tutorial.AddressBookProtos$PersonExt");
      System.out.println(cls.getField("extInfo").get(null));

      Object fieldValue = AddressBookProtos.class.getField("name").get(null);
      GeneratedExtension<AddressBook, ?> a = (GeneratedExtension<AddressBook, ?>)fieldValue;
      System.out.println(a.getDescriptor().getFullName());
      System.out.println(a.getDescriptor().getJavaType());
    } catch (IllegalArgumentException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
