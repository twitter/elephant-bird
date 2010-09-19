package com.twitter.elephantbird.examples.proto.hive.serde;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.examples.proto.mapreduce.io.ProtobufPersonWritable;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.io.Writable;
import com.twitter.elephantbird.hive.serde.LzoProtobufHiveSerde;

public class LzoPersonProtobufHiveSerde extends LzoProtobufHiveSerde {
  public ObjectInspector getObjectInspector() throws SerDeException {
    return ObjectInspectorFactory.getReflectionObjectInspector(Person.class, ObjectInspectorOptions.PROTOCOL_BUFFERS);
  }

  public Object deserialize(Writable w) throws SerDeException {
    return ((ProtobufPersonWritable) w).get();
  }
}

