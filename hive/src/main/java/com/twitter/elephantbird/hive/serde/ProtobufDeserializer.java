package com.twitter.elephantbird.hive.serde;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
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

  private ProtobufConverter<? extends Message> protobufConverter = null;
  private Class<? extends Message> protobufClass;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    try {
      String className = tbl.getProperty(Constants.SERIALIZATION_CLASS);
      protobufClass = conf.getClassByName(className).asSubclass(Message.class);
      protobufConverter = ProtobufConverter.newInstance(protobufClass);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable bytes = (BytesWritable)blob;
    return protobufConverter.fromBytes(bytes.getBytes(), 0, bytes.getLength());
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return ObjectInspectorFactory.getReflectionObjectInspector(
            protobufClass, ObjectInspectorFactory.ObjectInspectorOptions.PROTOCOL_BUFFERS);
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
