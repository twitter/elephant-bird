package com.twitter.elephantbird.hive.serde;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.ByteString;
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
    private FieldDescriptor fieldDescriptor = null;
    private Descriptor descriptor = null;
    private Message.Builder builder = null;

    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fieldDescriptor) {
      assert (fieldDescriptor != null);
      this.fieldDescriptor = fieldDescriptor;
      this.descriptor = null;
      oi = this.createOIForField();
    }

    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fd, Message.Builder builder) {
      this.descriptor = fd.getMessageType();
      this.fieldDescriptor = fd;
      this.builder = builder;
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
      ObjectInspector elementOI = null;
      if (descriptor != null) {
        // this field is a Message
        elementOI = new ProtobufStructObjectInspector(descriptor, builder);
      } else {
        JavaType fieldType = fieldDescriptor.getJavaType();
        PrimitiveCategory category = getPrimitiveCategory(fieldType);
        if (category != null) {
          elementOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
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
  private Message.Builder builder;
  private List<StructField> structFields = Lists.newArrayList();

  ProtobufStructObjectInspector(Descriptor descriptor, Message.Builder builder) {
    this.descriptor = descriptor;
    this.builder = builder;
    for (FieldDescriptor fd : descriptor.getFields()) {
      if (fd.getType() != Type.MESSAGE) {
        structFields.add(new ProtobufStructField(fd));
      } else {
        structFields.add(new ProtobufStructField(fd, builder.newBuilderForField(fd)));
      }
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
    return builder;
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object fieldValue) {
    Message.Builder builder = (Message.Builder) data;
    ProtobufStructField psf = (ProtobufStructField) field;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
    if (fieldDescriptor.getType() != Type.MESSAGE) {
      builder.setField(descriptor.findFieldByName(field.getFieldName()), fieldValue);
    } else {
      Message.Builder subFieldBuilder = (Message.Builder) fieldValue;
      builder.setField(descriptor.findFieldByName(field.getFieldName()), subFieldBuilder.build());
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
    Message.Builder m = ((Message.Builder) data);
    ProtobufStructField psf = (ProtobufStructField) structField;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();

    Object result = m.getField(fieldDescriptor);
    if (fieldDescriptor.getType() == Type.ENUM) {
      return ((EnumValueDescriptor) result).getName();
    }
    if (fieldDescriptor.getType() == Type.BYTES && (result instanceof ByteString)) {
      return ((ByteString) result).toByteArray();
    }

    if (fieldDescriptor.getType() == Type.MESSAGE && !fieldDescriptor.isRepeated()) {
      result = ((Message) result).toBuilder();
    }

    return result;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    FieldDescriptor fd = descriptor.findFieldByName(fieldName);
    ProtobufStructField result = null;
    if (fd.getType() != Type.MESSAGE) {
      result = new ProtobufStructField(fd);
    } else {
      result = new ProtobufStructField(fd, builder.newBuilderForField(fd));
    }
    return result;
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> result = Lists.newArrayList();
    Message.Builder m = (Message.Builder) data;
    for (FieldDescriptor fd : descriptor.getFields()) {
      result.add(m.getField(fd));
    }
    return result;
  }
}
