package com.twitter.elephantbird.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
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

  private static Map<Class<?>, TStructDescriptor> structMap = Maps.newHashMap();

  private List<Field> fields;
  private Class<? extends TBase<?, ?>> tClass;
  private boolean isUnion;

  public Class<? extends TBase<?, ?>> getThriftClass() {
    return tClass;
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
      arr[idx++] = new Field(e.getKey(),
                             e.getKey().getFieldName(),
                             tClass,
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
    Map<String, TEnum> map = Maps.newHashMapWithExpectedSize(enumClass.getEnumConstants().length);
    for(TEnum e : enumClass.getEnumConstants()) {
      map.put(e.toString(), e);
    }
    return map;
  }

  /**
   * Maintains all the relevant info for a field
   */
  public static class Field {

    private final TFieldIdEnum fieldIdEnum;
    private final short fieldId;
    private final String fieldName;
    private final FieldValueMetaData field;

    // following fields are set when they are relevant.
    private final Field listElemField;    // lists
    private final Field setElemField;     // sets
    private final Field mapKeyField;      // maps
    private final Field mapValueField;    // maps
    private final Map<String, TEnum> enumMap; // enums
    private final TStructDescriptor tStructDescriptor; // Structs
    private final boolean isBuffer_;  // strings


    @SuppressWarnings("unchecked") // for casting 'structClass' below
    private Field(TFieldIdEnum fieldIdEnum, String fieldName, Class<?> enclosingClass,
                  FieldValueMetaData field) {
      // enclosingClass is only to check a TType.STRING is actually a buffer.
      this.fieldIdEnum = fieldIdEnum;
      this.fieldId = fieldIdEnum == null ? 1 : fieldIdEnum.getThriftFieldId();
      this.fieldName = fieldName;
      this.field = field;

      // common case, avoids type checks below.
      boolean simpleField = field.getClass() == FieldValueMetaData.class;

      if (!simpleField && field instanceof ListMetaData) {
        listElemField = new Field(null, fieldName + "_list_elem", null,
                                  ((ListMetaData)field).elemMetaData);
      } else {
        listElemField = null;
      }

      if (!simpleField && field instanceof MapMetaData) {
        mapKeyField = new Field(null, fieldName + "_map_key", null,
                                ((MapMetaData)field).keyMetaData);
        mapValueField = new Field(null, fieldName + "_map_value", null,
                            ((MapMetaData)field).valueMetaData);

      } else {
        mapKeyField = null;
        mapValueField = null;
      }

      if (!simpleField && field instanceof SetMetaData) {
        setElemField = new Field(null, fieldName + "_set_elem", null,
                                ((SetMetaData)field).elemMetaData);
      } else {
        setElemField = null;
      }

      if (!simpleField && field instanceof EnumMetaData) {
        enumMap = extractEnumMap(((EnumMetaData)field).enumClass);
      } else {
        enumMap = null;
      }

      if (field.isStruct()) {
        tStructDescriptor =
          getInstance((Class<? extends TBase<?, ?>>)
                      ((StructMetaData)field).structClass);

      } else {
        tStructDescriptor = null;
      }

      if (field.type == TType.STRING && enclosingClass != null) {
        // only Thrift 0.6 and above have explicit isBuffer() method.
        // until then a partial work around that works only if
        // the field is not inside a container.
        isBuffer_ =
          ThriftUtils.getFieldType(enclosingClass, fieldName) != String.class;
      } else {
        isBuffer_= false;
      }
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
      return isBuffer_;
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

    public String getName() {
      return fieldName;
    }

    public short getId() {
     return fieldId;
    }
  }
}
