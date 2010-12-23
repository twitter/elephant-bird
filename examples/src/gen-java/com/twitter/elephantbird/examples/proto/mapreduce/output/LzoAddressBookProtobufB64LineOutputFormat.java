package com.twitter.elephantbird.examples.proto.mapreduce.output;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoAddressBookProtobufB64LineOutputFormat extends LzoProtobufB64LineOutputFormat<AddressBook> {
  public LzoAddressBookProtobufB64LineOutputFormat() {
    setTypeRef(new TypeRef<AddressBook>(){});
  }
}

