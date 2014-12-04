package com.twitter.elephantbird.hive.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.twitter.elephantbird.util.Protobufs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

/**
 * A deserializer for protobufs. Expects protbuf serialized bytes as
 * BytesWritables from input format. <p>
 *
 * Usage: <pre>
 *  create table users
 *    row format serde "com.twitter.elephantbird.hive.serde.ProtobufDeserializer"
 *    with serdeproperties (
 *        "serialization.class"="com.example.proto.gen.Storage$User")
 *    stored as
 *    inputformat "com.twitter.elephantbird.mapred.input.DeprecatedRawMultiInputFormat"
  *  ;
 * </pre>
 */
public class ProtobufDeserializer implements Deserializer {

  private Message.Builder msgBuilder;
  private ObjectInspector objectInspector;

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {
      String protoClassName = tbl
          .getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS);

      Class<? extends Message> protobufClass = job.getClassByName(protoClassName)
          .asSubclass(Message.class);
      msgBuilder = Protobufs.getMessageBuilder(protobufClass);
      Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
      objectInspector = new ProtobufStructObjectInspector(descriptor);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable bytes = (BytesWritable) blob;
    try {
      return msgBuilder.clear().mergeFrom(bytes.getBytes(), 0, bytes.getLength());
    } catch (InvalidProtocolBufferException e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
