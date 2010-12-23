package com.twitter.elephantbird.examples.proto.mapreduce.input;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoAddressBookProtobufBlockInputFormat extends LzoProtobufBlockInputFormat<AddressBook> {
  public LzoAddressBookProtobufBlockInputFormat() {
    setTypeRef(new TypeRef<AddressBook>(){});
  }
}

