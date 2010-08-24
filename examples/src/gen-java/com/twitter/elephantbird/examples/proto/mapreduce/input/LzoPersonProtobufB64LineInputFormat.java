package com.twitter.elephantbird.examples.proto.mapreduce.input;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.examples.proto.mapreduce.io.ProtobufPersonWritable;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufB64LineInputFormat;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufB64LineInputFormat extends LzoProtobufB64LineInputFormat<Person, ProtobufPersonWritable> {
  public LzoPersonProtobufB64LineInputFormat() {
    setTypeRef(new TypeRef<Person>(){});
    setProtobufWritable(new ProtobufPersonWritable());
  }
}

