package com.twitter.elephantbird.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

public class ThriftUtils {
  
  private static final String CLASS_CONF_PREFIX = "elephantbird.thirft.class.for.";
  
  public static void setClassConf(Configuration jobConf, Class<?> genericClass, 
                                  Class<? extends TBase<?>> thriftClass) {
    jobConf.set(CLASS_CONF_PREFIX + genericClass.getName(), thriftClass.getName());
  }
  
  /**
   * Returns TypeRef for the Thrift class that was set using setClass(jobConf);
   */  
  public static<M extends TBase<?>> TypeRef<M> getTypeRef(Configuration jobConf, Class<?> genericClass) {
    String className = jobConf.get(CLASS_CONF_PREFIX + genericClass.getName());
    if (className == null) {
      throw new RuntimeException(CLASS_CONF_PREFIX + genericClass.getName() + " is not set");
    }

    Class<?> tClass = null;    
    try {
      tClass = jobConf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    
    //how can we make sure tClass is Thrift class?
    //if (!tClass.isAssignableFrom(TBase.class)) {
    //  throw new RuntimeException(tClass.getName() + " is not a Thrift class");
    //}      
    
    return new TypeRef<M>(tClass){};  
  }
}
