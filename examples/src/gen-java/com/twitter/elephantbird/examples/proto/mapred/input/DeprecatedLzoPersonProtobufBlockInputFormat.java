package com.twitter.elephantbird.examples.proto.mapred.input;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.mapred.input.DeprecatedLzoProtobufBlockInputFormat;
import com.twitter.elephantbird.examples.proto.mapreduce.io.ProtobufPersonWritable;
import com.twitter.elephantbird.util.TypeRef;

public class DeprecatedLzoPersonProtobufBlockInputFormat extends DeprecatedLzoProtobufBlockInputFormat<Person, ProtobufPersonWritable> {
  public DeprecatedLzoPersonProtobufBlockInputFormat() {
    setTypeRef(new TypeRef<Person>(){});
    setProtobufWritable(new ProtobufPersonWritable());
  }
}

