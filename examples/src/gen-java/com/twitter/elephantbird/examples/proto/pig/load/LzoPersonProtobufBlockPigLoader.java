package com.twitter.elephantbird.examples.proto.pig.load;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.pig.load.LzoProtobufBlockPigLoader;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufBlockPigLoader extends LzoProtobufBlockPigLoader<Person> {
  public LzoPersonProtobufBlockPigLoader() {
    setTypeRef(new TypeRef<Person>(){});
  }
}

