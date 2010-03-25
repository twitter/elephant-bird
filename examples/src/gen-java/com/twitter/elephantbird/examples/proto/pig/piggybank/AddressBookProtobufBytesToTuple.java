package com.twitter.elephantbird.examples.proto.pig.piggybank;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.pig.piggybank.ProtobufBytesToTuple;
import com.twitter.elephantbird.util.TypeRef;

public class AddressBookProtobufBytesToTuple extends ProtobufBytesToTuple<AddressBook> {
  public AddressBookProtobufBytesToTuple() {
    setTypeRef(new TypeRef<AddressBook>(){});
  }
}

