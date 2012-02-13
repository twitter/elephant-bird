package com.twitter.elephantbird.examples.proto.mapreduce.output;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoAddressBookProtobufBlockOutputFormat extends LzoProtobufBlockOutputFormat<AddressBook> {
  public LzoAddressBookProtobufBlockOutputFormat() {
    super(new TypeRef<AddressBook>(){}, null);
  }
}

