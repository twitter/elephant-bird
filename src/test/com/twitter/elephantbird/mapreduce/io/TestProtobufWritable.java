package com.twitter.elephantbird.mapreduce.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.elephantbird.util.TypeRef;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestProtobufWritable {

  @Test
  public void testReadWrite() throws IOException {
    Person p1 = Person.newBuilder()
                      .setEmail("email1@example.com")
                      .setId(74)
                      .setName("Example Person")
                      .addPhone(PhoneNumber.newBuilder().setType(PhoneType.MOBILE).setNumber("2930423").build())
                      .addPhone(PhoneNumber.newBuilder().setType(PhoneType.HOME).setNumber("214121").build())
                      .build();

    Person p2 = Person.newBuilder()
                      .setEmail("email2@example.com")
                      .setId(7334)
                      .setName("Another person")
                      .addPhone(PhoneNumber.newBuilder().setType(PhoneType.MOBILE).setNumber("030303").build())
                      .build();

    AddressBook ab1 = AddressBook.newBuilder()
                                 .addPerson(p1)
                                 .addPerson(p2)
                                 .build();

    ProtobufWritable<AddressBook> before = new ProtobufWritable<AddressBook>(ab1, new TypeRef<AddressBook>(){});
    DataOutputStream dos = new DataOutputStream(new FileOutputStream("test.txt"));
    before.write(dos);
    dos.close();

    DataInputStream dis = new DataInputStream(new FileInputStream("test.txt"));
    ProtobufWritable<AddressBook> after = new ProtobufWritable<AddressBook>(new TypeRef<AddressBook>(){});
    after.readFields(dis);
    dis.close();

    AddressBook ab2 = after.get();
    assertEquals(ab1, ab2);
  }
}
