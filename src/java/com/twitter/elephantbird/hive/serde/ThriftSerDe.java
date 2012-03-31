package com.twitter.elephantbird.hive.serde;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

public class ThriftSerDe implements SerDe {
  private static final Log LOG = LogFactory.getLog(ThriftSerDe.class.getName());

  private String thriftClassName;
  private Class thriftClass;
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    thriftClassName = properties.getProperty(Constants.SERIALIZATION_CLASS, null);
    if (thriftClassName == null) {
      throw new SerDeException("Required property " + Constants.SERIALIZATION_CLASS + " is null.");
    }

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
   * @param writable any @{link ThriftWritable}
   * @return the actual thrift object
   * @throws SerDeException
   */
  @Override
  public Object deserialize(Writable writable) throws SerDeException {
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
