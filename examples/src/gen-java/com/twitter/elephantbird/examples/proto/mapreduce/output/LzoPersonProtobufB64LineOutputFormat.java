package com.twitter.elephantbird.examples.proto.mapreduce.output;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufB64LineOutputFormat extends LzoProtobufB64LineOutputFormat<Person> {
  public LzoPersonProtobufB64LineOutputFormat() {
    setTypeRef(new TypeRef<Person>(){});
  }
}

