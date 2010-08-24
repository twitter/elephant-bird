package com.twitter.elephantbird.examples.proto.mapreduce.input;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.examples.proto.mapreduce.io.ProtobufPersonWritable;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufBlockInputFormat extends LzoProtobufBlockInputFormat<Person, ProtobufPersonWritable> {
  public LzoPersonProtobufBlockInputFormat() {
    setTypeRef(new TypeRef<Person>(){});
    setProtobufWritable(new ProtobufPersonWritable());
  }
}

