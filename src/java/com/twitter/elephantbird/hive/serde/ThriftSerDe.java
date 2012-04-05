package com.twitter.elephantbird.hive.serde;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

/**
 * SerDe for working with {@link ThriftWritable} records.
 * This pairs well with {@link com.twitter.elephantbird.mapred.input.HiveMultiInputFormat}.
 */
public class ThriftSerDe implements SerDe {
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    String thriftClassName = properties.getProperty(Constants.SERIALIZATION_CLASS, null);
    if (thriftClassName == null) {
      throw new SerDeException("Required property " + Constants.SERIALIZATION_CLASS + " is null.");
    }

    Class thriftClass;
    try {
      thriftClass = conf.getClassByName(thriftClassName);
    } catch (ClassNotFoundException e) {
      throw new SerDeException("Failed getting class for " + thriftClassName);
    }

    inspector = ObjectInspectorFactory.getReflectionObjectInspector(
        thriftClass, ObjectInspectorFactory.ObjectInspectorOptions.THRIFT);
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    return null;
  }

  /**
   * @param writable the {@link ThriftWritable} to deserialize
   * @return the actual thrift object
   * @throws SerDeException
   */
  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    if (!(writable instanceof ThriftWritable)) {
      throw new SerDeException("Not an instance of ThriftWritable: " + writable);
    }
    return ((ThriftWritable) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
