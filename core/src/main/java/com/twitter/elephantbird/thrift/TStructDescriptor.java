package com.twitter.elephantbird.thrift;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.FieldMetaData;

import org.apache.thrift.protocol.TType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.twitter.elephantbird.util.ThriftUtils;

/**
 * Expanded metadata of a Thrift class. The main purpose is
 * help recursive traversals of Thrift structs or objects
 * with the following :
 * <ul>
 *  <li> build much more detailed information about fields so that
 *       other iterators don't need to.
 *  <li> avoids runtime type checking while processing many objects of the
 *       same class (common case).
 *  <li> Handles different Thrift quirks.
 *
 */
public class TStructDescriptor {

  private static final Map<Class<?>, TStructDescriptor> structMap = Maps.newHashMap();

  private List<Field> fields;
  private Class<? extends TBase<?, ?>> tClass;
  private boolean isUnion;

  public Class<? extends TBase<?, ?>> getThriftClass() {
    return tClass;
  }

  public TBase<?, ?> newThriftObject() throws TException {
    try {
      return tClass.newInstance();
    } catch (Exception e) { //not excpected
      throw new TException(e);
    }
  }
  /**
   * The list of fields returned is immutable.
   */
  public List<Field> getFields() {
    return fields;
  }

  public Field getFieldAt(int idx) {
    return fields.get(idx);
  }

  @SuppressWarnings("unchecked")
  public Object getFieldValue(int fieldIdx, TBase tObject) {
    /* Thrift 0.5 throws an NPE when the field is binary and
     * happens to be null. Otherwise user could just
     * invoke tObject.getFieldValue(field.getFieldIdEnum()).
     * Should revisit this once we move to a newer version.
     *
     * here, the assumption is the field is not null most of the
     * time. otherwise, rather than catching the exception, we
     * could cache 'BufferForFieldName()' method and invoke it.
     *
     * this also helps with unions.
     */
    Field field = fields.get(fieldIdx);
    try {
      if (isUnion && field.getFieldIdEnum() != ((TUnion<?, ?>)tObject).getSetField()) {
        return null;
      }
      return tObject.getFieldValue(field.getFieldIdEnum());
    } catch (NullPointerException e) {
      return null;
    }
  }

  /**
   * Creates a descriptor for a Thrift class
   */
  public static TStructDescriptor getInstance(Class<? extends TBase<?, ?>> tClass) {
    synchronized (structMap) {
      TStructDescriptor desc = structMap.get(tClass);
      if (desc == null) {
        desc = new TStructDescriptor();
        desc.tClass = tClass;
        structMap.put(tClass, desc);
        desc.build(tClass);
      }
      return desc;
    }
  }

  private TStructDescriptor() {
    // all the initialization is done in build().
  }

  private void build(Class<? extends TBase<?, ?>> tClass) {
    Map<? extends TFieldIdEnum, FieldMetaData> fieldMap = FieldMetaData.getStructMetaDataMap(tClass);
    Field[] arr = new Field[fieldMap.size()];

    isUnion = TUnion.class.isAssignableFrom(tClass);

    int idx = 0;
    for (Entry<? extends TFieldIdEnum, FieldMetaData> e : fieldMap.entrySet()) {
      String fieldName = e.getKey().getFieldName();
      arr[idx++] = new Field(e.getKey(),
                             e.getValue(),
                             fieldName,
                             ThriftUtils.getFieldType(tClass, fieldName));
    }
    // make it immutable since users have access.
    fields = ImmutableList.copyOf(arr);
  }

  /**
   * returns 'enum name -> enum object' mapping.
   * Currently used for converting Tuple to a Thrift object.
   */
  static private Map<String, TEnum> extractEnumMap(Class<? extends TEnum> enumClass) {
    ImmutableMap.Builder<String, TEnum> builder = ImmutableMap.builder();
    for(TEnum e : enumClass.getEnumConstants()) {
      builder.put(e.toString(), e);
    }
    return builder.build();
  }

  /**
   * Maintains all the relevant info for a field
   */
  public static class Field {

    private final TFieldIdEnum fieldIdEnum;
    private final FieldMetaData fieldMetaData;
    private final short fieldId;
    private final String fieldName;
    private final byte ttype;

    // following fields are set when they are relevant.
    // though these are not declared final, this class should be immutable.
    private Field listElemField                 = null;   // lists
    private Field setElemField                  = null;   // sets
    private Field mapKeyField                   = null;   // maps
    private Field mapValueField                 = null;   // maps
    private Class<? extends TEnum> enumClass    = null;   // enums
    private Map<String, TEnum> enumMap          = null;   // enums
    private Map<Integer, TEnum> enumIdMap       = null;   // enums
    private TStructDescriptor tStructDescriptor = null;   // Structs
    private boolean isBuffer                    = false;  // strings

    @SuppressWarnings("unchecked")
    private Field(TFieldIdEnum fieldIdEnum, FieldMetaData fieldMetaData, String fieldName,
                  Type genericType) {
      this.fieldIdEnum = fieldIdEnum;
      this.fieldMetaData = fieldMetaData;
      this.fieldId = fieldIdEnum == null ? 1 : fieldIdEnum.getThriftFieldId();
      this.fieldName = fieldName;
      this.ttype = getTTypeFromJavaType(genericType);

      Type firstTypeArg = null;
      Type secondTypeArg = null;

      if (genericType instanceof ParameterizedType) {
        Type[] typeArgs = ((ParameterizedType) genericType).getActualTypeArguments();
        firstTypeArg = typeArgs.length > 0 ? typeArgs[0] : null;
        secondTypeArg = typeArgs.length > 1 ? typeArgs[1] : null;
      }

      switch (ttype) {

        case TType.LIST:
          listElemField = new Field(null, null, fieldName + "_list_elem", firstTypeArg);
          break;

        case TType.MAP:
          mapKeyField = new Field(null, null, fieldName + "_map_key", firstTypeArg);
          mapValueField = new Field(null, null, fieldName + "_map_value", secondTypeArg);
          break;

        case TType.SET:
          setElemField = new Field(null, null, fieldName + "_set_elem", firstTypeArg);
          break;

        case TType.ENUM:
          enumClass = (Class<? extends TEnum>)genericType;
          enumMap = extractEnumMap(enumClass);

          ImmutableMap.Builder<Integer, TEnum> builder = ImmutableMap.builder();
          for(TEnum e : enumMap.values()) {
            builder.put(e.getValue(), e);
          }
          enumIdMap = builder.build();
          break;

        case TType.STRUCT:
          tStructDescriptor = getInstance((Class<? extends TBase<?, ?>>) genericType);
          break;

        case TType.STRING:
          isBuffer = !genericType.equals(String.class);
          break;

        default:
          // this is ok. TType.INT, TType.BYTE, etc.
      }
    }

    public short getFieldId() {
      return fieldId;
    }

    public byte getType() {
      return ttype;
    }

    /**
     * This is valid only for fields of a Struct. It is null for other fields
     * in containers like List.
     */
    public TFieldIdEnum getFieldIdEnum() {
      return fieldIdEnum;
    }

    public boolean isBuffer() {
      return isBuffer;
    }

    public boolean isList() {
      return listElemField != null;
    }

    public Field getListElemField() {
      return listElemField;
    }

    public boolean isSet() {
      return setElemField != null;
    }

    public Field getSetElemField() {
      return setElemField;
    }

    public boolean isMap() {
      return mapKeyField != null;
    }

    public Field getMapKeyField() {
      return mapKeyField;
    }

    public Field getMapValueField() {
      return mapValueField;
    }

    public boolean isStruct() {
      return tStructDescriptor != null;
    }

    public TStructDescriptor gettStructDescriptor() {
      return tStructDescriptor;
    }

    /**
     *
     * @return true if the field is a known enum.
     */
    public boolean isEnum() {
      return enumClass != null;
    }

    public Class<? extends TEnum> getEnumClass() {
      return enumClass;
    }

    /**
     * @param name string value of the enum
     * @return the TEnum value of the named enum, or null
     * if the field is not a known enum.
     */
    public TEnum getEnumValueOf(String name) {
      return enumMap == null ? null : enumMap.get(name);
    }

    /**
     * @param id int value of the enum
     * @return the TEnum value of the named enum, or null
     * if the field is not a known enum.
     */
    public TEnum getEnumValueOf(int id) {
      return enumIdMap == null ? null : enumIdMap.get(id);
    }

    /**
     * @return the possible values of the enum field, or null
     * if the field is not a known enum.
     */
    public Collection<TEnum> getEnumValues() {
      return enumMap == null ? null : enumMap.values();
    }

    public String getName() {
      return fieldName;
    }

    public short getId() {
     return fieldId;
    }

    public FieldMetaData getFieldMetaData() {
      return fieldMetaData;
    }
  }

  /** Derives Thrift TType from java {@link Type} of a thrift field */
  private static byte getTTypeFromJavaType(Type jType) {

    // check primitive types and final classes
    if (jType == Boolean.TYPE || jType == Boolean.class)   return TType.BOOL;
    if (jType == Byte.TYPE    || jType == Byte.class)      return TType.BYTE;
    if (jType == Short.TYPE   || jType == Short.class)     return TType.I16;
    if (jType == Integer.TYPE || jType == Integer.class)   return TType.I32;
    if (jType == Long.TYPE    || jType == Long.class)      return TType.I64;
    if (jType == Double.TYPE  || jType == Double.class)    return TType.DOUBLE;
    if (jType == String.class)                             return TType.STRING;
    if (jType == byte[].class)                             return TType.STRING; // buffer
    if (jType == Void.class)                               return TType.VOID;

    // non-generic simple classes
    if (jType instanceof Class) {
      Class<?> klass = (Class<?>) jType;

      if (TEnum.class.isAssignableFrom(klass))      return TType.ENUM;
      if (ByteBuffer.class.isAssignableFrom(klass)) return TType.STRING; // buffer (ByteBuffer)
      if (TBase.class.isAssignableFrom(klass))      return TType.STRUCT;
    }

    // generic types
    if (jType instanceof ParameterizedType) {
      Class<?> klass = (Class<?>) ((ParameterizedType)jType).getRawType();

      if (Map.class.isAssignableFrom(klass))    return TType.MAP;
      if (Set.class.isAssignableFrom(klass))    return TType.SET;
      if (List.class.isAssignableFrom(klass))   return TType.LIST;
    }

    throw new IllegalArgumentException("cannot convert java type '" + jType + "'  to thrift type");
  }
}
