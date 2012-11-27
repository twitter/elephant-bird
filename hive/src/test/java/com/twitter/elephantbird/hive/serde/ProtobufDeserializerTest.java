package com.twitter.elephantbird.hive.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.elephantbird.hive.serde.ProtobufStructObjectInspector.ProtobufStructField;

public class ProtobufDeserializerTest {

  private AddressBook test_ab;
  private PhoneNumber test_pn;
  private ProtobufDeserializer deserializer;
  private ProtobufStructObjectInspector protobufOI;

  @Before
  public void setUp() throws SerDeException {
    PhoneNumber pn1 = PhoneNumber.newBuilder().setNumber("pn0001").setType(PhoneType.HOME).build();
    PhoneNumber pn2 = PhoneNumber.newBuilder().setNumber("pn0002").setType(PhoneType.WORK).build();
    PhoneNumber pn3 = PhoneNumber.newBuilder().setNumber("pn0003").build();
    test_pn = PhoneNumber.newBuilder().setNumber("pn0004").setType(PhoneType.MOBILE)
        .build();

    Person p1 = Person.newBuilder().setName("p1").setId(1).setEmail("p1@twitter").addPhone(pn1)
        .addPhone(pn2).addPhone(pn3).build();
    Person p2 = Person.newBuilder().setName("p2").setId(2).addPhone(test_pn).build();
    Person p3 = Person.newBuilder().setName("p3").setId(3).build();

    test_ab = AddressBook.newBuilder().addPerson(p1).addPerson(p2).addPerson(p3).build();
    deserializer = new ProtobufDeserializer();

    Properties properties = new Properties();
    properties.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS,
        AddressBook.class.getName());
    deserializer.initialize(new Configuration(), properties);
    protobufOI = (ProtobufStructObjectInspector) deserializer.getObjectInspector();
  }

  @Test
  public final void testDeserializer() throws SerDeException {
    BytesWritable serialized = new BytesWritable(test_ab.toByteArray());
    AddressBook ab2 = (AddressBook) deserializer.deserialize(serialized);
    assertTrue(test_ab.equals(ab2));
  }

  @Test
  public final void testObjectInspector() throws SerDeException {
    ObjectInspector oi = deserializer.getObjectInspector();
    assertEquals(oi.getCategory(), Category.STRUCT);

    ProtobufStructObjectInspector protobufOI = (ProtobufStructObjectInspector) oi;
    List<Object> readData = protobufOI.getStructFieldsDataAsList(test_ab);

    assertEquals(readData.size(), 1);
    @SuppressWarnings("unchecked")
    List<Person> persons = (List<Person>) readData.get(0);
    assertEquals(persons.size(), 3);
    assertEquals(persons.get(0).getPhoneCount(), 3);
    assertEquals(persons.get(0).getPhone(2).getType(), PhoneType.HOME);
    assertEquals(persons.get(0).getId(), 1);

    assertEquals(persons.get(1).getPhoneCount(), 1);
    assertEquals(persons.get(1).getPhone(0), test_pn);
    assertEquals(persons.get(1).getPhone(0).getType(), PhoneType.MOBILE);

    assertEquals(persons.get(2).getPhoneCount(), 0);
    assertEquals(persons.get(2).getId(), 3);
    assertEquals(persons.get(2).getEmail(), "");
  }

  @Test
  public final void testObjectInspectorGetStructFieldData() throws SerDeException {
    checkFields(AddressBook.getDescriptor().getFields(), test_ab);
    checkFields(PhoneNumber.getDescriptor().getFields(), test_pn);
  }

  private void checkFields(List<FieldDescriptor> fields, Message message) {
    for (FieldDescriptor fieldDescriptor : fields) {
      ProtobufStructField psf = new ProtobufStructField(fieldDescriptor);
      Object data = protobufOI.getStructFieldData(message, psf);
      Object target = message.getField(fieldDescriptor);
      if (fieldDescriptor.getType() == Type.ENUM) {
        assertEquals(String.class, data.getClass());
        assertEquals(data, ((EnumValueDescriptor) target).getName());
      } else {
        assertEquals(data, target);
      }
    }
  }

  @Test
  public final void testObjectInspectorGetTypeName() throws SerDeException {
    ProtobufStructObjectInspector protobufOI = (ProtobufStructObjectInspector) deserializer
        .getObjectInspector();
    assertEquals(protobufOI.getTypeName(),
        "struct<person:array<"
            + "struct<name:string,id:int,email:string,"
            + "phone:array<struct<number:string,type:string>>>>>");
  }

  @Test
  public final void testElementObjectInspector() throws SerDeException {
    ProtobufStructObjectInspector protobufOI = (ProtobufStructObjectInspector) deserializer
        .getObjectInspector();

    ProtobufStructObjectInspector personOI = new ProtobufStructObjectInspector(
        Person.getDescriptor());
    assertEquals(protobufOI.getStructFieldRef("person").getFieldObjectInspector().getClass(),
        ObjectInspectorFactory.getStandardListObjectInspector(personOI).getClass());

    StandardListObjectInspector phoneOI = (StandardListObjectInspector) personOI.getStructFieldRef(
        "phone").getFieldObjectInspector();
    assertEquals(phoneOI.getListElementObjectInspector().getTypeName(),
        "struct<number:string,type:string>");
  }
}
