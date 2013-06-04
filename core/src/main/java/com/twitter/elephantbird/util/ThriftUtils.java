package com.twitter.elephantbird.util;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class ThriftUtils {

  private static final String CLASS_CONF_PREFIX = "elephantbird.thrift.class.for.";

  public static void setClassConf(Configuration jobConf, Class<?> genericClass,
                                  Class<? extends TBase<?, ?>> thriftClass) {
    String configKey = CLASS_CONF_PREFIX + genericClass.getName();
    HadoopUtils.setClassConf(jobConf, configKey, thriftClass);
  }

  /**
   * Verify that clazz is a Thrift class. i.e. is a subclass of TBase
   */
  private static void verifyAncestry(Class<?> tClass) {
    if (!TBase.class.isAssignableFrom(tClass)) {
      Utils.ensureClassLoaderConsistency(TBase.class, tClass.getClassLoader());
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
      java.lang.reflect.Field field = containingClass.getDeclaredField(fieldName);
      return fieldClass.cast(field.get(obj));
    } catch (Exception e) {
      throw new RuntimeException("while trying to find " + fieldName + " in "
                                  +  containingClass.getName(), e);
    }
  }

  /**
   * Returns gereric type for a field in a Thrift class. The type is the return
   * type for the accessor method for the field (e.g. <code>isFieldName()</code>
   * for a boolean type or <code>getFieldName</code> for other types). The return
   * type works for both structs and unions. Reflecting directly based on
   * fields does not work for unions.
   *
   * @return generic {@link Type} of the thrift field.
   */
  public static Type getFieldType(Class<?> containingClass, String fieldName) {

    String suffix = // uppercase first letter
        fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

    // look for getFieldName() or isFieldName()

    for(String prefix : new String[]{"get", "is"}) {
      try {
        Method method = containingClass.getDeclaredMethod(prefix + suffix);
        return method.getGenericReturnType();
      } catch (NoSuchMethodException e) {
      }
    }

    throw new RuntimeException("could not find type for " + fieldName +
                                 " in " + containingClass);
  }

  /**
   * Returns the value class of the given thrift field.
   *
   * @param field thrift field.
   * @return class of field value, or null in the case of thrift types {@link TType#STOP} or
   * {@link TType#VOID}.
   */
  public static Class<?> getFieldValueType(Field field) {
    switch (field.getType()) {
      case TType.BOOL:
        return Boolean.class;
      case TType.BYTE:
        return Byte.class;
      case TType.DOUBLE:
        return Double.class;
      case TType.ENUM:
        return ((EnumMetaData) field.getField()).enumClass;
      case TType.I16:
        return Short.class;
      case TType.I32:
        return Integer.class;
      case TType.I64:
        return Long.class;
      case TType.LIST:
        return List.class;
      case TType.MAP:
        return Map.class;
      case TType.SET:
        return Set.class;
      case TType.STOP:
        return null;
      case TType.STRING:
        return String.class;
      case TType.STRUCT:
        return field.gettStructDescriptor().getThriftClass();
      case TType.VOID:
        return null;
    }
    return null;
  }

  private static void writeSingleFieldNoTag(TProtocol proto,
                                            Field field,
                                            Object value) throws TException {
    switch(field.getType()) {

    case TType.BOOL:
      proto.writeBool((Boolean)value);              break;
    case TType.BYTE:
      proto.writeByte((Byte)value);                 break;
    case TType.I16:
      proto.writeI16((Short)value);                 break;
    case TType.I32:
      proto.writeI32((Integer)value);               break;
    case TType.ENUM:
      proto.writeI32(((TEnum)value).getValue());    break;
    case TType.I64:
      proto.writeI64((Long)value);                  break;
    case TType.DOUBLE:
      proto.writeDouble((Double)value);             break;
    case TType.STRING: {
      if (value instanceof String) {
        proto.writeString((String)value);
      } else {
        proto.writeBinary((ByteBuffer)value);
      }
    }                                               break;
    case TType.STRUCT:
      ((TBase<?, ?>)value).write(proto);            break;

    default:
      throw new IllegalArgumentException("Unexpected type : " + field.getType());
    }
  }

  /**
   * Serializes a single field of a thrift struct.
   *
   * @throws TException
   */
  public static void writeFieldNoTag(TProtocol proto,
                                     Field field,
                                     Object value) throws TException {
    if (value == null) {
      return;
    }

    Field innerField = null;

    switch (field.getType()) {

    case TType.LIST:
      innerField = field.getListElemField();    break;
    case TType.SET:
      innerField = field.getSetElemField();     break;
    case TType.MAP:
      innerField = field.getMapKeyField();      break;

    default:
      writeSingleFieldNoTag(proto, field, value);
      return;
    }

    // a map or a collection:

    if (field.getType() == TType.MAP) {

      Field valueField = field.getMapValueField();
      Map<?, ?> map = (Map<?, ?>)value;

      proto.writeByte(innerField.getType());
      proto.writeByte(valueField.getType());
      proto.writeI32(map.size());

      for(Entry<?, ?> entry : map.entrySet()) {
        writeSingleFieldNoTag(proto, innerField, entry.getKey());
        writeSingleFieldNoTag(proto, valueField, entry.getValue());
      }

    } else { // SET or LIST

      Collection<?> coll = (Collection<?>)value;

      proto.writeByte(innerField.getType());
      proto.writeI32(coll.size());

      for(Object v : coll) {
        writeSingleFieldNoTag(proto, innerField, v);
      }

    }
  }

  private static Object readSingleFieldNoTag(TProtocol  proto,
                                             Field      field)
                                             throws TException {
    switch(field.getType()) {

    case TType.BOOL:
      return proto.readBool();
    case TType.BYTE:
      return proto.readByte();
    case TType.I16:
      return proto.readI16();
    case TType.I32:
      return proto.readI32();
    case TType.ENUM:
      return field.getEnumValueOf(proto.readI32());
    case TType.I64:
      return proto.readI64();
    case TType.DOUBLE:
      return proto.readDouble();
    case TType.STRING:
      return field.isBuffer() ?  proto.readBinary() : proto.readString();
    case TType.STRUCT:
      TBase<?, ?> tObj = field.gettStructDescriptor().newThriftObject();
      tObj.read(proto);
      return tObj;

    default:
      throw new IllegalArgumentException("Unexpected type : " + field.getType());
    }

  }

  /**
   * Deserializes a thrift field that was serilized with
   * {@link #writeFieldNoTag(TProtocol, Field, Object)}.
   *
   * @throws TException in case of any Thrift errors.
   */
  public static Object readFieldNoTag(TProtocol   proto,
                                      Field       field)
                                      throws TException {

    Collection<Object> coll = null;
    Field innerField = null;

    switch (field.getType()) {

    case TType.LIST:
      innerField = field.getListElemField();
      coll = Lists.newArrayList();              break;
    case TType.SET:
      innerField = field.getSetElemField();
      coll = Sets.newHashSet();                 break;
    case TType.MAP:
      innerField = field.getMapKeyField();      break;

    default:
      return readSingleFieldNoTag(proto, field);
    }

    // collection or a map:


    if (field.getType() == TType.MAP) {

      proto.readByte();
      proto.readByte();
      int nEntries = proto.readI32();

      Map<Object, Object> map = Maps.newHashMap();
      Field valueField = field.getMapValueField();

      for (int i=0; i<nEntries; i++) {
        map.put(readFieldNoTag(proto, innerField),
                readFieldNoTag(proto, valueField));
      }
      return map;

    } else { // SET or LIST

      proto.readByte();
      int nEntries = proto.readI32();

      for(int i=0; i<nEntries; i++) {
        coll.add(readFieldNoTag(proto, innerField));
      }
      return coll;

    }
  }
}
