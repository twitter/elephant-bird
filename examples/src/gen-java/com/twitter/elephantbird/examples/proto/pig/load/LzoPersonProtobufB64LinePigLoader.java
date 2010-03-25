package com.twitter.elephantbird.examples.proto.pig.load;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.pig.load.LzoProtobufB64LinePigLoader;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufB64LinePigLoader extends LzoProtobufB64LinePigLoader<Person> {
  public LzoPersonProtobufB64LinePigLoader() {
    setTypeRef(new TypeRef<Person>(){});
  }
}

