package com.twitter.elephantbird.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TType;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

/**
 * Translates a Thrift object into a Protocol Buffer message.
 * <p>
 * Enums are converted to their String representations.
 * </p>
 * TODO: handle complex fields. Only primitives and enums are mapped right now.<br>
 *
 * @param <T> Source thrift class
 */
public class ThriftToDynamicProto<T extends TBase<?,?>> {

  DynamicMessage.Builder protoBuilder;
  Descriptors.Descriptor msgDescriptor;
  Message protoObj;


  /**
   * Create a Dynamic Protocol Buffer message with fields equivalent to those in the provided Thrift class.
   * @param thriftClass
   * @throws DescriptorValidationException
   */
  public ThriftToDynamicProto(Class<T> thriftClass) throws DescriptorValidationException {
    this(thriftClass, new ArrayList<Pair<String, Type>>());
  }

  /**
   * Create a Dynamic Protocol Buffer message with fields equivalent to those in the provided Thrift class,
   * plus additional fields as defined by extraFields.
   * @param thriftClass
   * @param extraFields a list of pairs of (fieldName, protobufType) that defines extra fields to add to
   * the generated message.
   * @throws DescriptorValidationException
   */
  public ThriftToDynamicProto(Class<T> thriftClass, List<Pair<String, Type>> extraFields)
  throws DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder desBuilder = DescriptorProtos.DescriptorProto.newBuilder();
    desBuilder.setName("dynaMessage");
    TStructDescriptor fieldDesc = TStructDescriptor.getInstance(thriftClass);
    int maxThriftId = 0;
    // Protocol field ids start at 1. Thrift starts at 0.
    for (Field tField : fieldDesc.getFields()) {
      Type protoType = thriftTypeToProtoType(tField);
      maxThriftId = Math.max(tField.getFieldId(), maxThriftId);
      addField(desBuilder, tField.getName(), tField.getFieldId() + 1, protoType);
    }
    int extraFieldIdx = maxThriftId + 1;
    for (Pair<String, Type> extraField : extraFields) {
      addField(desBuilder, extraField.getFirst(), ++extraFieldIdx, extraField.getSecond());
    }
    msgDescriptor = Protobufs.makeMessageDescriptor(desBuilder.build());
    protoBuilder = DynamicMessage.newBuilder(msgDescriptor);
  }

  private void addField(DescriptorProtos.DescriptorProto.Builder builder, String name, int fieldIdx, Type type) {
    DescriptorProtos.FieldDescriptorProto.Builder fdBuilder =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
      .setName(name)
      .setNumber(fieldIdx)
      .setType(type);
    builder.addField(fdBuilder.build());
  }

  @SuppressWarnings("unchecked")
  public DynamicMessage convert(T thriftObj) {
    DynamicMessage.Builder builder = protoBuilder.clone();

    TStructDescriptor fieldDesc = TStructDescriptor.getInstance((Class<? extends TBase<?, ?>>) thriftObj.getClass());
    int fieldId = 0;
    for (Field tField : fieldDesc.getFields()) {
      FieldDescriptor protoFieldDesc = msgDescriptor.findFieldByName(tField.getName());
      if ( protoFieldDesc == null ) {
        throw new RuntimeException("Field " + tField.getName() +
            " not found in dynamic protobuf.");
      }
      Object fieldValue = fieldDesc.getFieldValue(fieldId++, thriftObj);
      if (fieldValue == null) {
        continue;
      }
      if (tField.isEnum()) {
        // TODO: proper enum handling
        fieldValue = fieldValue.toString();
      } else if ( tField.isBuffer() ) {
        fieldValue = ByteString.copyFrom((byte [])fieldValue);
      }
      builder.setField(protoFieldDesc, fieldValue);
    }
    return builder.build();
  }

  private  Type thriftTypeToProtoType(Field tField) {
    byte thriftType = tField.getType();
    switch (thriftType)  {
      case TType.BOOL:
        return Type.TYPE_BOOL;
      case TType.BYTE:
        return Type.TYPE_INT32;
      case TType.DOUBLE:
        return Type.TYPE_DOUBLE;
      case TType.I16:
        return Type.TYPE_INT32;
      case TType.I32:
        return Type.TYPE_INT32;
      case TType.I64:
        return Type.TYPE_INT64;
      case TType.STRING:
        // Thrift thinks bytes and strings are interchangeable. Protocol buffers are not insane.
        return tField.isBuffer() ? Type.TYPE_BYTES : Type.TYPE_STRING;
      case TType.ENUM:
        // TODO: proper enum handling. For now, convert to strings.
        return Type.TYPE_STRING;
      default:
        throw new IllegalArgumentException("Can't map Thrift type " + thriftType + " to a ProtoBuf type");
    }
  }

}

