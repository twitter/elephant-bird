package com.twitter.elephantbird.examples.proto.mapreduce.input;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufB64LineInputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoAddressBookProtobufB64LineInputFormat extends LzoProtobufB64LineInputFormat<AddressBook> {
  public LzoAddressBookProtobufB64LineInputFormat() {
    setTypeRef(new TypeRef<AddressBook>(){});
  }
}

