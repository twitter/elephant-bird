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

import edu.emory.mathcs.backport.java.util.Arrays;

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

  public static AddressBook buildAddressBookProto() {
    AddressBook abProto = AddressBook.newBuilder()
    .addPerson(buildPersonProto())
    .addPerson(buildPersonProto())
    .setExtension(AddressBookProtos.name, "private")
    .build();
    return abProto;
  }

  public static Person buildPersonProto() {
    return Person.newBuilder()
    .addPhone(makePhoneNumber("415-999-9999"))
    .addPhone(makePhoneNumber("415-666-6666",PhoneType.MOBILE))
    .addPhone(makePhoneNumber("415-333-3333", PhoneType.WORK))
    .setEmail("elephant@bird.com")
    .setName("Elephant Bird")
    .setId(123)
    .setExtension(PersonExt.extInfo, PersonExt.newBuilder().setAddress("Rd. Foo").build())
    .setExtension(JustPersonExtExtEnclosingType.extExtInfo, "bar")
    .build();
  }

  public static Tuple buildPersonTuple() throws ExecException {
    DataBag phoneBag = bf_.newDefaultBag();
    phoneBag.add(makePhoneNumberTuple("415-999-9999", null));
    phoneBag.add(makePhoneNumberTuple("415-666-6666", "MOBILE"));
    phoneBag.add(makePhoneNumberTuple("415-333-3333", "WORK"));

    Tuple entryTuple = tf_.newTuple(6);
    entryTuple.set(0, "Elephant Bird");
    entryTuple.set(1, 123);
    entryTuple.set(2, "elephant@bird.com");
    entryTuple.set(3, phoneBag);
    entryTuple.set(4, tf_.newTuple("Rd. Foo"));
    entryTuple.set(5, "bar");
    return entryTuple;
  }
  // {(Elephant Bird,123,elephant@bird.com,{(415-999-9999,HOME),(415-666-6666,MOBILE),(415-333-3333,WORK)})}
  public static Tuple buildAddressBookTuple() throws ExecException {
    DataBag entryBag = bf_.newDefaultBag();
    entryBag.add(buildPersonTuple());
    entryBag.add(buildPersonTuple());
    return tf_.newTuple(Arrays.asList(new Object[]{entryBag, "private"}));
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
