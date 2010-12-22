package com.twitter.elephantbird.examples.proto.pig.piggybank;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.pig8.piggybank.ProtobufBytesToTuple;
import com.twitter.elephantbird.util.TypeRef;

public class PersonProtobufBytesToTuple extends ProtobufBytesToTuple<Person> {
  public PersonProtobufBytesToTuple() {
    setTypeRef(new TypeRef<Person>(){});
  }
}

