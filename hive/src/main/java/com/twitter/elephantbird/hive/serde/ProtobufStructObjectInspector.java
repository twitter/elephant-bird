package com.twitter.elephantbird.hive.serde;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public final class ProtobufStructObjectInspector extends SettableStructObjectInspector {

  public static class ProtobufStructField implements StructField {

    private ObjectInspector oi = null;
    private String comment = null;
    private FieldDescriptor fieldDescriptor;

    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      oi = this.createOIForField();
    }

    @Override
    public String getFieldName() {
      return fieldDescriptor.getName();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String getFieldComment() {
      return comment;
    }

    public FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
    }

    private PrimitiveCategory getPrimitiveCategory(JavaType fieldType) {
      switch (fieldType) {
        case INT:
          return PrimitiveCategory.INT;
        case LONG:
          return PrimitiveCategory.LONG;
        case FLOAT:
          return PrimitiveCategory.FLOAT;
        case DOUBLE:
          return PrimitiveCategory.DOUBLE;
        case BOOLEAN:
          return PrimitiveCategory.BOOLEAN;
        case STRING:
          return PrimitiveCategory.STRING;
        case BYTE_STRING:
          return PrimitiveCategory.BINARY;
        case ENUM:
          return PrimitiveCategory.STRING;
        default:
          return null;
      }
    }

    private ObjectInspector createOIForField() {
      JavaType fieldType = fieldDescriptor.getJavaType();
      PrimitiveCategory category = getPrimitiveCategory(fieldType);
      ObjectInspector elementOI = null;
      if (category != null) {
        elementOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
      } else {
        switch (fieldType) {
          case MESSAGE:
            elementOI = new ProtobufStructObjectInspector(fieldDescriptor.getMessageType());
            break;
          default:
            throw new RuntimeException("JavaType " + fieldType
                + " from protobuf is not supported.");
        }
      }
      if (fieldDescriptor.isRepeated()) {
        return ObjectInspectorFactory.getStandardListObjectInspector(elementOI);
      } else {
        return elementOI;
      }
    }
  }

  private Descriptor descriptor;
  private List<StructField> structFields = Lists.newArrayList();

  ProtobufStructObjectInspector(Descriptor descriptor) {
    this.descriptor = descriptor;
    for (FieldDescriptor fd : descriptor.getFields()) {
      structFields.add(new ProtobufStructField(fd));
    }
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    StringBuilder sb = new StringBuilder("struct<");
    boolean first = true;
    for (StructField structField : getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(structField.getFieldName()).append(":")
          .append(structField.getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  @Override
  public Object create() {
    return descriptor.toProto().toBuilder().build();
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object fieldValue) {
    return ((Message) data)
        .toBuilder()
        .setField(descriptor.findFieldByName(field.getFieldName()), fieldValue)
        .build();
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField structField) {
    if (data == null) {
      return null;
    }
    Message m = (Message) data;
    ProtobufStructField psf = (ProtobufStructField) structField;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
    Object result = m.getField(fieldDescriptor);
    if (fieldDescriptor.getType() == Type.ENUM) {
      return ((EnumValueDescriptor)result).getName();
    }
    return result;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return new ProtobufStructField(descriptor.findFieldByName(fieldName));
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> result = Lists.newArrayList();
    Message m = (Message) data;
    for (FieldDescriptor fd : descriptor.getFields()) {
      result.add(m.getField(fd));
    }
    return result;
  }
}
