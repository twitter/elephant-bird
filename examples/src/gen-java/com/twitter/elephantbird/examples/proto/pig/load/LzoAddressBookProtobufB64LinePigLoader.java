package com.twitter.elephantbird.examples.proto.pig.load;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.pig.load.LzoProtobufB64LinePigLoader;
import com.twitter.elephantbird.util.TypeRef;

public class LzoAddressBookProtobufB64LinePigLoader extends LzoProtobufB64LinePigLoader<AddressBook> {
  public LzoAddressBookProtobufB64LinePigLoader() {
    setTypeRef(new TypeRef<AddressBook>(){});
  }
}

