package com.twitter.elephantbird.examples.proto.mapreduce.io;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

public class ProtobufAddressBookWritable extends ProtobufWritable<AddressBook> {
  public ProtobufAddressBookWritable() {
    super(new TypeRef<AddressBook>(){});
  }
  public ProtobufAddressBookWritable(AddressBook m) {
    super(m, new TypeRef<AddressBook>(){});
  }
}

