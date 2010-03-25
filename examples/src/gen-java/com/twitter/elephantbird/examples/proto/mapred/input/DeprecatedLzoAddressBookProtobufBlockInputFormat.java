package com.twitter.elephantbird.examples.proto.mapred.input;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.mapred.input.DeprecatedLzoProtobufBlockInputFormat;
import com.twitter.elephantbird.examples.proto.mapreduce.io.ProtobufAddressBookWritable;
import com.twitter.elephantbird.util.TypeRef;

public class DeprecatedLzoAddressBookProtobufBlockInputFormat extends DeprecatedLzoProtobufBlockInputFormat<AddressBook, ProtobufAddressBookWritable> {
  public DeprecatedLzoAddressBookProtobufBlockInputFormat() {
    setTypeRef(new TypeRef<AddressBook>(){});
    setProtobufWritable(new ProtobufAddressBookWritable());
  }
}

