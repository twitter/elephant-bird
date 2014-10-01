package com.twitter.elephantbird.util;

import java.util.List;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Metadata stored with columnar storage like Hive's RCFile <p>
 *
 * This is a {@link DynamicMessage} equivalent of following protobuf : <pre>
 *
 * message ColumnarMetadata {
 *   optional string   classname   = 1;                // FYI only, not used.
 *   repeated int32    fieldId     = 2;                // list of field ids stored
 *   optional int32    nesting     = 3 [default = 0];  // when nesting is used
 * };
 * </pre>
 */
public class ColumnarMetadata {

  private final Message message;

  // use newInstance() to create a new message
  private ColumnarMetadata(Message message) {
    this.message = message;
  }

  public Message getMessage() {
    return message;
  }

  public String getClassname() {
    return (String) message.getField(classnameDesc);
  }

  public int getNesting() {
    return (Integer) message.getField(nestingDesc);
  }

  public int getFieldId(int index) {
    return getFieldIdList().get(index);
  }

  public List<Integer> getFieldIdList() {
    return (List<Integer>) message.getField(fieldIdDesc);
  }

  public static ColumnarMetadata newInstance(String classname, List<Integer> fieldIdList) {
    return newInstance(classname, fieldIdList, 0);
  }

  public static ColumnarMetadata newInstance(String classname, List<Integer> fieldIdList, int nesting) {
    return new ColumnarMetadata(
        DynamicMessage.newBuilder(messageDescriptor)
            .setField(classnameDesc, classname)
            .setField(fieldIdDesc, fieldIdList)
            .setField(nestingDesc, nesting)
            .build());
  }

  public static ColumnarMetadata parseFrom(byte[] messageBuffer)
      throws InvalidProtocolBufferException {
    return new ColumnarMetadata(
        DynamicMessage.newBuilder(messageDescriptor)
            .mergeFrom(messageBuffer)
            .build());
  }

  private static final Descriptors.Descriptor messageDescriptor;
  private static final Descriptors.FieldDescriptor classnameDesc;
  private static final Descriptors.FieldDescriptor fieldIdDesc;
  private static final Descriptors.FieldDescriptor nestingDesc;

  static {
    // initialize messageDescriptor and the three field descriptors

    DescriptorProtos.FieldDescriptorProto classname =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("classname")
            .setNumber(1)
            .setType(Type.TYPE_STRING)
            .setLabel(Label.LABEL_OPTIONAL)
            .build();

    DescriptorProtos.FieldDescriptorProto fieldId =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("fieldId")
            .setNumber(2)
            .setType(Type.TYPE_INT32)
            .setLabel(Label.LABEL_REPEATED)
            .build();

    DescriptorProtos.FieldDescriptorProto nesting =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("nesting")
            .setNumber(3)
            .setType(Type.TYPE_INT32)
            .setLabel(Label.LABEL_OPTIONAL)
            .setDefaultValue("0")
            .build();

    try {
      messageDescriptor = Protobufs.makeMessageDescriptor(
          DescriptorProtos.DescriptorProto.newBuilder()
              .setName("ColumnarMetadata")
              .addField(classname)
              .addField(fieldId)
              .addField(nesting)
              .build());
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }

    classnameDesc   = messageDescriptor.findFieldByName("classname");
    fieldIdDesc     = messageDescriptor.findFieldByName("fieldId");
    nestingDesc     = messageDescriptor.findFieldByName("nesting");
  }
}
