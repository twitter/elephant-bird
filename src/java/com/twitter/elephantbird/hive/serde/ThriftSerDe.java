package com.twitter.elephantbird.hive.serde;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.util.Codecs;
import org.apache.commons.codec.binary.Base64;
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
  private static final Base64 base64 = Codecs.createStandardBase64();
  private ThriftConverter converter;
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

    converter = ThriftConverter.newInstance(thriftClass);

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
   * @param writable a base64 encoded serialized thrift object
   * @return the actual thrift object
   * @throws SerDeException
   */
  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    try {
      byte[] bytes = writable.toString().getBytes("UTF-8");
      return converter.fromBytes(base64.decode(bytes));
    } catch (Exception e) {
      throw new SerDeException(e);
    }
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
