package com.twitter.elephantbird.pig.piggybank;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;

public class Fixtures {

  public static TupleFactory tf_ = TupleFactory.getInstance();
  public static BagFactory bf_ = BagFactory.getInstance();

  public static AddressBook buildAddressBookProto() {
    Person personProto = Person.newBuilder()
    .addPhone(makePhoneNumber("415-999-9999"))
    .addPhone(makePhoneNumber("415-666-6666",PhoneType.MOBILE))
    .addPhone(makePhoneNumber("415-333-3333", PhoneType.WORK))
    .setEmail("elephant@bird.com")
    .setName("Elephant Bird")
    .setId(123)
    .build();
    AddressBook abProto = AddressBook.newBuilder().addPerson(personProto).build();
    return abProto;
  }
  
  // {(Elephant Bird,123,elephant@bird.com,{(415-999-9999,HOME),(415-666-6666,MOBILE),(415-333-3333,WORK)})}
  public static Tuple buildAddressBookTuple() throws ExecException {
    DataBag phoneBag = bf_.newDefaultBag();
    phoneBag.add(makePhoneNumberTuple("415-999-9999", null));
    phoneBag.add(makePhoneNumberTuple("415-666-6666", "MOBILE"));
    phoneBag.add(makePhoneNumberTuple("415-333-3333", "WORK"));
    
    Tuple entryTuple = tf_.newTuple(4);
    entryTuple.set(0, "Elephant Bird");
    entryTuple.set(1, 123);
    entryTuple.set(2, "elephant@bird.com");
    entryTuple.set(3, phoneBag);
   
    DataBag entryBag = bf_.newDefaultBag();
    entryBag.add(entryTuple);
    return tf_.newTuple(entryBag);
  }

  private static Tuple makePhoneNumberTuple(String number, String type) throws ExecException {
    Tuple t = tf_.newTuple(2);
    t.set(0, number);
    t.set(1, type);
    return t;
  }
  private static PhoneNumber makePhoneNumber(String number) {
    return makePhoneNumber(number, null);
  }
  
  private static PhoneNumber makePhoneNumber(String number, PhoneType type) {
    PhoneNumber.Builder phoneBuilder = PhoneNumber.newBuilder();
    phoneBuilder.setNumber(number);
    if (type != null) {
      phoneBuilder.setType(type);
    }
    return phoneBuilder.build();
  }
}
