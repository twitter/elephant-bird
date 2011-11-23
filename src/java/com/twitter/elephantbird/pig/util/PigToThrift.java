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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TType;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
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
    }
    return tObj;
  }

  @SuppressWarnings("unchecked")
  private static Object toThriftValue(Field field, Object value) {
    try {
      switch (field.getType()) {
      case TType.BOOL:
        return Boolean.valueOf(((Integer)value) != 0);
      case TType.BYTE :
        return ((Integer)value).byteValue();
      case TType.I16 :
        return Short.valueOf(((Integer)value).shortValue());
      case TType.STRING:
        return toStringType(value);
      case TType.STRUCT:
        return toThrift(field.gettStructDescriptor(), (Tuple)value);
      case TType.MAP:
        return toThriftMap(field, (Map<String, Object>)value);
      case TType.SET:
        return toThriftSet(field.getSetElemField(), (DataBag) value);
      case TType.LIST:
        return toThriftList(field.getListElemField(), (DataBag)value);
      case TType.ENUM:
        return field.getEnumValueOf(value.toString());
      default:
        // standard types : I32, I64, DOUBLE, etc.
        return value;
      }
    } catch (Exception e) {
      // mostly a schema mismatch.
      ThriftToPig.LOG.warn("Exception while convering Tuple to Thrift. "
          + " from " + value.getClass() + " to " + field.getName()
          + (field.getFieldIdEnum() == null ? "" :
            "(field id : " + field.getFieldIdEnum().getClass() + ")"), e);
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
