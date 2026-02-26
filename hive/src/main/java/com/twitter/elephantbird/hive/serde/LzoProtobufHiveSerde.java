package com.twitter.elephantbird.hive.serde;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

public abstract class LzoProtobufHiveSerde implements SerDe {
  
  @Override
  public void initialize(Configuration conf, Properties props) throws SerDeException {
  }
  
  @Override
  public abstract ObjectInspector getObjectInspector() throws SerDeException;

  @Override
  public abstract Object deserialize(Writable w) throws SerDeException;

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return org.apache.hadoop.io.Text.class; 
    //serialization not supported
  }

  @Override
  public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
    return null;
    //serialization not supported
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
    // stats not supported
  }
}
