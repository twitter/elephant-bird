package com.twitter.elephantbird.util;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

public class ThriftUtils {

  private static final String CLASS_CONF_PREFIX = "elephantbird.thrift.class.for.";

  public static void setClassConf(Configuration jobConf, Class<?> genericClass,
                                  Class<? extends TBase<?, ?>> thriftClass) {
    jobConf.set(CLASS_CONF_PREFIX + genericClass.getName(), thriftClass.getName());
  }


  /**
   * Verify that clazz is a Thrift class. i.e. is a subclass of TBase
   */
  private static void verifyAncestry(Class<?> tClass) {
    if (!TBase.class.isAssignableFrom(tClass)) {
      throw new ClassCastException(tClass.getName() + " is not a Thrift class");
    }
  }

  /**
   * Returns TypeRef for the Thrift class that was set using setClass(jobConf);
   */
  public static<M extends TBase<?, ?>> TypeRef<M> getTypeRef(Configuration jobConf, Class<?> genericClass) {
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

    verifyAncestry(tClass);

    return new TypeRef<M>(tClass){};
  }

   /**
    * returns TypeRef for thrift class. Verifies that the class is
    * a Thrift class (i.e extends TBase).
    */
  public static <M extends TBase<?, ?>> TypeRef<M> getTypeRef(Class<?> tClass) {
    verifyAncestry(tClass);
    return new TypeRef<M>(tClass){};
  }

  /**
   * returns TypeRef for a thrift class.
   */
  public static <M extends TBase<?, ?>> TypeRef<M> getTypeRef(String thriftClassName, ClassLoader classLoader) {
    try {
      Class<?> tClass = classLoader == null ?
          Class.forName(thriftClassName) :
          Class.forName(thriftClassName, true, classLoader);

      verifyAncestry(tClass);

      return new TypeRef<M>(tClass){};
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * returns TypeRef for a thrift class.
   */
  public static<M extends TBase<?, ?>> TypeRef<M> getTypeRef(String thriftClassName) {
    return getTypeRef(thriftClassName, null);
  }

   /**
   * Returns value of a fieldName in an object.
   */
  public static <M> M getFieldValue(Object containingObject, String fieldName, Class<M> fieldClass) {
    return getFieldValue(containingObject.getClass(), containingObject, fieldName, fieldClass);
  }

  /**
   * Returns value of a static field with given name in containingClass.
   */
  public static <M> M getFieldValue(Class<?> containingClass, String fieldName, Class<M> fieldClass) {
    return getFieldValue(containingClass, null, fieldName, fieldClass);
  }

  private static <M> M getFieldValue(Class<?> containingClass, Object obj, String fieldName, Class<M> fieldClass) {
    try {
      Field field = containingClass.getDeclaredField(fieldName);
      return fieldClass.cast(field.get(obj));
    } catch (Exception e) {
      throw new RuntimeException("while trying to find " + fieldName + " in "
                                  +  containingClass.getName(), e);
    }
  }

  public static Class<?> getFiedlType(Class<?> containingClass, String fieldName) {
    try {
      Field field = containingClass.getDeclaredField(fieldName);
      return field.getType();
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("while trying to find " + fieldName + " in "
                                 + containingClass, e);
    }
  }
}
