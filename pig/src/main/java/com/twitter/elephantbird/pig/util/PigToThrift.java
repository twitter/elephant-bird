package com.twitter.elephantbird.pig.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.protocol.TType;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Converts a Pig Tuple into a Thrift struct. Tuple values should be ordered to match the natural
 * order of Thrift field ordinal values. For example, say we define the following Thrift struct:
 *
 * <pre>
 * struct MyThriftType {
 *   1: i32 f1
 *   3: i32 f2
 *   7: i32 f3
 * }
 * </pre>
 *
 * Input Tuples are expected to contain field values in order {@code (f1, f2, f3)}. Tuples may
 * contain fewer values than Thrift struct fields (e.g. only {@code (f1, f2)} in the prior example);
 * Any remaining fields will be left unset.
 */
public class PigToThrift<T extends TBase<?, ?>> {
  public static final Logger LOG = LoggerFactory.getLogger(PigToThrift.class);

  private TStructDescriptor structDesc;

  public static <T extends TBase<?, ?>> PigToThrift<T> newInstance(Class<T> tClass) {
    return new PigToThrift<T>(tClass);
  }

  public static <T extends TBase<?, ?>> PigToThrift<T> newInstance(TypeRef<T> typeRef) {
    return new PigToThrift<T>(typeRef.getRawClass());
  }

  public PigToThrift(Class<T> tClass) {
    structDesc = TStructDescriptor.getInstance(tClass);
    // may be TODO : compare the schemas to catch errors early.
  }

  @SuppressWarnings("unchecked")
  public T getThriftObject(Tuple tuple) {
    return (T)toThrift(structDesc, tuple);
  }

  /**
   * Construct a Thrift object from the tuple.
   */
  @SuppressWarnings("unchecked")
  private static TBase<?, ?> toThrift(TStructDescriptor tDesc, Tuple tuple) {
    int size = tDesc.getFields().size();
    int tupleSize = tuple.size();
    @SuppressWarnings("rawtypes")
    TBase tObj = newTInstance(tDesc.getThriftClass());
    for(int i = 0; i<size && i<tupleSize; i++) {
      Object pObj;
      try {
        pObj = tuple.get(i);
      } catch (ExecException e) {
        throw new RuntimeException(e);
      }
      if (pObj != null) {
        Field field = tDesc.getFieldAt(i);
        try {
          tObj.setFieldValue(field.getFieldIdEnum(), toThriftValue(field, pObj));
        } catch (Exception e) {
          String value = String.valueOf(tObj);
          final int max_length = 100;
          if (max_length < value.length()) {
            value = value.substring(0, max_length - 3) + "...";
          }
          String type = tObj == null ? "unknown" : tObj.getClass().getName();
          throw new RuntimeException(String.format(
              "Failed to set field '%s' using tuple value '%s' of type '%s' at index %d",
              field.getName(), value, type, i), e);
        }
      }
      // if tDesc is a union, at least one field needs to be non-null.
      // user is responsible for ensuring that.
    }
    return tObj;
  }

  /**
   * For a given Pig value, return a Thrift object of the same type as the Thrift field passed. The
   * thrift field is expected to be compatible with the value passed. If it is not, a warning will
   * be logged and a null value will be returned.
   *
   * @param thriftField the Thrift field used to determine the type of the response object
   * @param pigValue the value to convert to Thrift
   * @return a Thrift object
   */
  @SuppressWarnings("unchecked")
  public static Object toThriftValue(Field thriftField, Object pigValue) {
    try {
      switch (thriftField.getType()) {
      case TType.BOOL:
        return Boolean.valueOf(((Integer)pigValue) != 0);
      case TType.BYTE :
        return ((Integer)pigValue).byteValue();
      case TType.I16 :
        return Short.valueOf(((Integer)pigValue).shortValue());
      case TType.STRING:
        return toStringType(pigValue);
      case TType.STRUCT:
        return toThrift(thriftField.gettStructDescriptor(), (Tuple)pigValue);
      case TType.MAP:
        return toThriftMap(thriftField, (Map<String, Object>)pigValue);
      case TType.SET:
        return toThriftSet(thriftField.getSetElemField(), (DataBag) pigValue);
      case TType.LIST:
        return toThriftList(thriftField.getListElemField(), (DataBag)pigValue);
      case TType.ENUM:
        return toThriftEnum(thriftField, (String) pigValue);
      default:
        // standard types : I32, I64, DOUBLE, etc.
        return pigValue;
      }
    } catch (Exception e) {
      // mostly a schema mismatch.
      LOG.warn(String.format(
          "Failed to set field '%s' of type '%s' with value '%s' of type '%s'",
          thriftField.getName(), ThriftUtils.getFieldValueType(thriftField).getName(),
          pigValue, pigValue.getClass().getName()), e);
    }
    return null;
  }

  /* TType.STRING could be either a DataByteArray or a String */
  private static Object toStringType(Object value) {
    if (value instanceof String) {
      return value;
    } else if (value instanceof DataByteArray) {
      byte[] buf = ((DataByteArray)value).get();
      // mostly there is no need to copy.
      return ByteBuffer.wrap(Arrays.copyOf(buf, buf.length));
    }
    return null;
  }

  private static Map<Object, Object> toThriftMap(Field field, Map<String, Object> map) {
    Field keyField   = field.getMapKeyField();
    Field valueField = field.getMapValueField();
    if (keyField.getType() != TType.STRING
        && keyField.getType() != TType.ENUM) {
      throw new IllegalArgumentException("TStructs's map key should be a STRING or an ENUM");
    }
    HashMap<Object, Object> out = new HashMap<Object, Object>(map.size());
    for(Entry<String, Object> e : map.entrySet()) {
      out.put(toThriftValue(keyField,   e.getKey()),
              toThriftValue(valueField, e.getValue()));
    }
    return out;
  }

  private static Set<Object> toThriftSet(Field elemField, DataBag bag) {
    Set<Object> set = new HashSet<Object>((int)bag.size());
    fillThriftCollection(set, elemField, bag);
    return set;
  }

  private static List<Object> toThriftList(Field elemField, DataBag bag) {
    List<Object> list = new ArrayList<Object>((int)bag.size());
    fillThriftCollection(list, elemField, bag);
    return list;
  }

  private static TEnum toThriftEnum(Field elemField, String name) {
    TEnum out = elemField.getEnumValueOf(name);
    if (out == null) {
      throw new IllegalArgumentException(
          String.format("Failed to convert string '%s'" +
              " to enum value of type '%s'", name,
              ThriftUtils.getFieldValueType(elemField).getName()));
    }
    return out;
  }

  private static void fillThriftCollection(Collection<Object> tColl, Field elemField, DataBag bag) {
    for (Tuple tuple : bag) {
      if (!elemField.isStruct()) {
        // this tuple is a just wrapper for another object.
        try {
          tColl.add(toThriftValue(elemField, tuple.get(0)));
        } catch (ExecException e) {
          throw new RuntimeException(e);
        }
      } else { // tuple for a struct.
        tColl.add(toThriftValue(elemField, tuple));
      }
    }
  }

  /** return an instance assuming tClass is a Thrift class */
  private static TBase<?, ?> newTInstance(Class<?> tClass) {
    try {
      return (TBase<?, ?>) tClass.newInstance();
    } catch (Exception e) { // not expected.
      throw new RuntimeException(e);
    }
  }
}
