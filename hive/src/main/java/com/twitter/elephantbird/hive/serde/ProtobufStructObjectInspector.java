package com.twitter.elephantbird.hive.serde;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
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
      comment = fieldDescriptor.getFullName();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ProtobufStructField) {
        ProtobufStructField other = (ProtobufStructField)obj;
        return fieldDescriptor.equals(other.fieldDescriptor);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return fieldDescriptor.hashCode();
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
  public boolean equals(Object obj) {
    if (obj instanceof ProtobufStructObjectInspector) {
      ProtobufStructObjectInspector other = (ProtobufStructObjectInspector)obj;
      return this.descriptor.equals(other.descriptor) &&
              this.structFields.equals(other.structFields);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return descriptor.hashCode();
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
    return DynamicMessage.newBuilder(descriptor);
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object fieldValue) {
    DynamicMessage.Builder builder = (DynamicMessage.Builder)data;
    ProtobufStructField psf = (ProtobufStructField)field;
    FieldDescriptor fd = psf.getFieldDescriptor();
    if (fd.isRepeated()) {
      return builder.setField(fd, fieldValue);
    }
    switch (fd.getType()) {
      case ENUM:
        builder.setField(fd, fd.getEnumType().findValueByName((String) fieldValue));
        break;
      case BYTES:
        builder.setField(fd, ByteString.copyFrom((byte[])fieldValue));
        break;
      case MESSAGE:
        builder.setField(fd, ((Message.Builder)fieldValue).build());
        break;
      default:
        builder.setField(fd, fieldValue);
        break;
    }
    return builder;
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
    Message.Builder builder = (Message.Builder) data;
    ProtobufStructField psf = (ProtobufStructField) structField;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
    Object result = builder.getField(fieldDescriptor);

    if (fieldDescriptor.isRepeated()) {
      return result;
    }

    if (fieldDescriptor.getType() == Type.ENUM) {
      return ((EnumValueDescriptor)result).getName();
    }
    if (fieldDescriptor.getType() == Type.BYTES && (result instanceof ByteString)) {
        return ((ByteString)result).toByteArray();
    }
    if (fieldDescriptor.getType() == Type.MESSAGE) {
      return ((Message)result).toBuilder();
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
    for (StructField field : structFields) {
      result.add(getStructFieldData(data, field));
    }
    return result;
  }
}
