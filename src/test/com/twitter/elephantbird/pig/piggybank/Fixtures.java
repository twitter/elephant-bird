package com.twitter.elephantbird.pig.piggybank;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.twitter.data.proto.tutorial.AddressBookProtos;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.JustPersonExtExtEnclosingType;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.data.proto.tutorial.AddressBookProtos.PersonExt;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;

public class Fixtures {

  public static class AddressBookExtensionRegistry extends ProtobufExtensionRegistry {
    public AddressBookExtensionRegistry() {
      addExtension(PersonExt.extInfo);
      addExtension(JustPersonExtExtEnclosingType.extExtInfo);
      addExtension(AddressBookProtos.name);
    }
  }

  public static TupleFactory tf_ = TupleFactory.getInstance();
  public static BagFactory bf_ = BagFactory.getInstance();

  public static ProtobufExtensionRegistry buildExtensionRegistry() {
    return new AddressBookExtensionRegistry();
  }

  public static AddressBook buildAddressBookProto(boolean withExtension) {
    AddressBook.Builder abProtoBuilder = AddressBook.newBuilder()
    .addPerson(buildPersonProto(withExtension))
    .addPerson(buildPersonProto(withExtension));
    if(withExtension) {
      abProtoBuilder.setExtension(AddressBookProtos.name, "private");
    }
    return abProtoBuilder.build();
  }

  public static Person buildPersonProto(boolean withExtension) {
    Person.Builder personBuilder = Person.newBuilder()
    .addPhone(makePhoneNumber("415-999-9999"))
    .addPhone(makePhoneNumber("415-666-6666",PhoneType.MOBILE))
    .addPhone(makePhoneNumber("415-333-3333", PhoneType.WORK))
    .setEmail("elephant@bird.com")
    .setName("Elephant Bird")
    .setId(123);
    if(withExtension) {
      personBuilder.setExtension(PersonExt.extInfo, PersonExt.newBuilder().setAddress("Rd. Foo").build())
      .setExtension(JustPersonExtExtEnclosingType.extExtInfo, "bar");
    }

    return personBuilder.build();
  }

  public static Tuple buildPersonTuple(boolean withExtension) throws ExecException {
    DataBag phoneBag = bf_.newDefaultBag();
    phoneBag.add(makePhoneNumberTuple("415-999-9999", null));
    phoneBag.add(makePhoneNumberTuple("415-666-6666", "MOBILE"));
    phoneBag.add(makePhoneNumberTuple("415-333-3333", "WORK"));

    Tuple entryTuple = tf_.newTuple(4);
    entryTuple.set(0, "Elephant Bird");
    entryTuple.set(1, 123);
    entryTuple.set(2, "elephant@bird.com");
    entryTuple.set(3, phoneBag);
    if(withExtension) {
      entryTuple.append(tf_.newTuple("Rd. Foo"));
      entryTuple.append("bar");
    }
    return entryTuple;
  }
  // {(Elephant Bird,123,elephant@bird.com,{(415-999-9999,HOME),(415-666-6666,MOBILE),(415-333-3333,WORK)})}
  public static Tuple buildAddressBookTuple(boolean withExtension) throws ExecException {
    DataBag entryBag = bf_.newDefaultBag();
    entryBag.add(buildPersonTuple(withExtension));
    entryBag.add(buildPersonTuple(withExtension));
    Tuple tuple = tf_.newTuple(1);
    tuple.set(0, entryBag);
    if(withExtension) {
      tuple.append("private");
    }
    return tuple;
  }

  public static Tuple makePhoneNumberTuple(String number, String type) throws ExecException {
    Tuple t = tf_.newTuple(2);
    t.set(0, number);
    t.set(1, type);
    return t;
  }
  public static PhoneNumber makePhoneNumber(String number) {
    return makePhoneNumber(number, null);
  }

  public static PhoneNumber makePhoneNumber(String number, PhoneType type) {
    PhoneNumber.Builder phoneBuilder = PhoneNumber.newBuilder();
    phoneBuilder.setNumber(number);
    if (type != null) {
      phoneBuilder.setType(type);
    }
    return phoneBuilder.build();
  }
}
