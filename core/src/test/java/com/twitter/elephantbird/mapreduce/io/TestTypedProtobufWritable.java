package com.twitter.elephantbird.mapreduce.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import com.google.protobuf.Message;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Java;
import org.apache.tools.ant.types.Environment;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;


public class TestTypedProtobufWritable {

  static AddressBook referenceAb;
  static TypedProtobufWritable<AddressBook> referenceAbWritable;

  @BeforeClass
  public static void setUp() {
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

    referenceAb = AddressBook.newBuilder()
    .addPerson(p1)
    .addPerson(p2)
    .build();

    referenceAbWritable =
      new TypedProtobufWritable<AddressBook>(referenceAb);
  }

  @Test
  public void testReadWrite() throws IOException {

    DataOutputStream dos = new DataOutputStream(new FileOutputStream("test.txt"));
    referenceAbWritable.write(dos);
    dos.close();

    DataInputStream dis = new DataInputStream(new FileInputStream("test.txt"));
    TypedProtobufWritable<AddressBook> after = new TypedProtobufWritable<AddressBook>();
    after.readFields(dis);
    dis.close();

    AddressBook ab2 = after.get();
    assertEquals(referenceAb, ab2);
    assertEquals(referenceAbWritable.hashCode(), after.hashCode());
  }
  
  @Test
  public void testMessageReadWrite() throws IOException {

    DataOutputStream dos = new DataOutputStream(new FileOutputStream("test2.txt"));
    referenceAbWritable.write(dos);
    dos.close();

    DataInputStream dis = new DataInputStream(new FileInputStream("test2.txt"));
    TypedProtobufWritable<Message> after = new TypedProtobufWritable<Message>();
    after.readFields(dis);
    dis.close();

    AddressBook ab2 = (AddressBook) after.get();
    assertEquals(referenceAb, ab2);
    assertEquals(referenceAbWritable.hashCode(), after.hashCode());
  }
  
  @Test
  public void testMessageReadWriteEmpty() throws IOException {

    DataOutputStream dos = new DataOutputStream(new FileOutputStream("test3.txt"));
    TypedProtobufWritable<AddressBook> empty = new TypedProtobufWritable<AddressBook>();
    empty.write(dos);
    dos.close();

    DataInputStream dis = new DataInputStream(new FileInputStream("test3.txt"));
    TypedProtobufWritable<Message> after = new TypedProtobufWritable<Message>();
    after.readFields(dis);
    dis.close();

    AddressBook ab2 = (AddressBook) after.get();
    assertNull(ab2);
  }

  @Test
  public void testStableHashcodeAcrossJVMs() throws IOException {
    int expectedHashCode = referenceAbWritable.hashCode();
    Java otherJvm = new Java();
    otherJvm.setNewenvironment(true);
    otherJvm.setFork(true);
    otherJvm.setProject(new Project());
    otherJvm.setClassname(OtherJvmClass.class.getName());
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      Environment.Variable var = new Environment.Variable();
      var.setKey(entry.getKey());
      var.setValue(entry.getValue());
      otherJvm.addEnv(var);
    }
    for (String prop : System.getProperties().stringPropertyNames()) {
      String propValue = System.getProperty(prop);
      Environment.Variable var = new Environment.Variable();
      var.setKey(prop);
      var.setValue(propValue);
      otherJvm.addSysproperty(var);
    }
    otherJvm.setDir(new File(System.getProperty("java.io.tmpdir")));
    File tmpOut = File.createTempFile("otherJvm", "txt");
    otherJvm.setArgs(tmpOut.getAbsolutePath());
    otherJvm.init();
    otherJvm.executeJava();
    DataInputStream is = new DataInputStream(new FileInputStream(tmpOut));
    assertEquals(expectedHashCode, is.readInt());
    is.close();
  }

  public static class OtherJvmClass {
    /* Used for testStableHashcodeAcrossJVMs */
    public static void main(String[] args) throws IOException {
      setUp();
      int hashCode = referenceAbWritable.hashCode();
      File tmpFile = new File(args[0]);
      DataOutputStream os = new DataOutputStream(new FileOutputStream(tmpFile));
      os.writeInt(hashCode);
      os.close();
      System.exit(0);
    }
  }
}
