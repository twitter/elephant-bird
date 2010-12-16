package com.twitter.elephantbird.examples.proto.mapreduce.io;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

public class ProtobufPersonWritable extends ProtobufWritable<Person> {
  public ProtobufPersonWritable() {
    super(new TypeRef<Person>(){});
  }
  public ProtobufPersonWritable(Person m) {
    super(m, new TypeRef<Person>(){});
  }
}

