package com.twitter.elephantbird.examples.proto.pig.load;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.AddressBook;
import com.twitter.elephantbird.pig8.load.LzoProtobufBlockPigLoader;
import com.twitter.elephantbird.util.TypeRef;

public class LzoAddressBookProtobufBlockPigLoader extends LzoProtobufBlockPigLoader<AddressBook> {
  public LzoAddressBookProtobufBlockPigLoader() {
    setTypeRef(new TypeRef<AddressBook>(){});
  }
}

