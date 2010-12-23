package com.twitter.elephantbird.examples.proto.mapreduce.output;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.examples.proto.mapreduce.io.ProtobufPersonWritable;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufBlockOutputFormat extends LzoProtobufBlockOutputFormat<Person, ProtobufPersonWritable> {
  public LzoPersonProtobufBlockOutputFormat() {
    setTypeRef(new TypeRef<Person>(){});
  }
}

