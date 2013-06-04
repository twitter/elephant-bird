package com.twitter.elephantbird.thrift;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
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
                             ThriftUtils.getFieldType(tClass, fieldName),
                             e.getValue().valueMetaData);
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
    private final FieldValueMetaData field;

    // following fields are set when they are relevant.
    private final Field listElemField;    // lists
    private final Field setElemField;     // sets
    private final Field mapKeyField;      // maps
    private final Field mapValueField;    // maps
    private final Map<String, TEnum> enumMap; // enums
    private final Map<Integer, TEnum> enumIdMap; // enums
    private final TStructDescriptor tStructDescriptor; // Structs
    private final boolean isBuffer;  // strings


    @SuppressWarnings("unchecked")
    private Field(TFieldIdEnum fieldIdEnum, FieldMetaData fieldMetaData, String fieldName,
                  Type genericType, FieldValueMetaData field) {
      this.fieldIdEnum = fieldIdEnum;
      this.fieldMetaData = fieldMetaData;
      this.fieldId = fieldIdEnum == null ? 1 : fieldIdEnum.getThriftFieldId();
      this.fieldName = fieldName;
      this.field = field;

      // common case, avoids type checks below.
      boolean simpleField = field.getClass() == FieldValueMetaData.class;

      Type firstTypeArg = null;
      Type secondTypeArg = null;

      if (genericType instanceof ParameterizedType) {
        Type[] typeArgs = ((ParameterizedType) genericType).getActualTypeArguments();
        firstTypeArg = typeArgs.length > 0 ? typeArgs[0] : null;
        secondTypeArg = typeArgs.length > 1 ? typeArgs[1] : null;
      }

      if (!simpleField && field instanceof ListMetaData) {
        listElemField = new Field(null, null, fieldName + "_list_elem", firstTypeArg,
                                  ((ListMetaData)field).elemMetaData);
      } else {
        listElemField = null;
      }

      if (!simpleField && field instanceof MapMetaData) {
        mapKeyField = new Field(null, null, fieldName + "_map_key", firstTypeArg,
                                ((MapMetaData)field).keyMetaData);
        mapValueField = new Field(null, null, fieldName + "_map_value", secondTypeArg,
                            ((MapMetaData)field).valueMetaData);
      } else {
        mapKeyField = null;
        mapValueField = null;
      }

      if (!simpleField && field instanceof SetMetaData) {
        setElemField = new Field(null, null, fieldName + "_set_elem", firstTypeArg,
                                ((SetMetaData)field).elemMetaData);
      } else {
        setElemField = null;
      }

      if (!simpleField && field instanceof EnumMetaData) {
        enumMap = extractEnumMap(((EnumMetaData)field).enumClass);

        ImmutableMap.Builder<Integer, TEnum> builder = ImmutableMap.builder();
        for(TEnum e : enumMap.values()) {
          builder.put(e.getValue(), e);
        }
        enumIdMap = builder.build();
      } else {
        enumMap = null;
        enumIdMap = null;
      }

      if (field.isStruct()) {
        tStructDescriptor = getInstance((Class<? extends TBase<?, ?>>) genericType);
      } else {
        tStructDescriptor = null;
      }

      // only Thrift 0.6 and above have explicit isBuffer() method.
      // just check the type instead
      isBuffer = field.type == TType.STRING
               && !genericType.equals(String.class);
    }

    public short getFieldId() {
      return fieldId;
    }

    public byte getType() {
      return field.type;
    }

    /**
     * This valid only for fields of a Struct. It is null for other fields
     * in containers like List.
     */
    public TFieldIdEnum getFieldIdEnum() {
      return fieldIdEnum;
    }

    public FieldValueMetaData getField() {
      return field;
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

    public boolean isEnum() {
      return enumMap != null;
    }

    public TEnum getEnumValueOf(String name) {
      return enumMap.get(name);
    }

    public TEnum getEnumValueOf(int id) {
      return enumIdMap.get(id);
    }

    public Collection<TEnum> getEnumValues() {
      return enumMap.values();
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
}
