package com.twitter.elephantbird.pig.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.Protobufs;

/**
 * Converts a Pig Tuple into a Protobuf message. Tuple values should be ordered to match the natural
 * order of Protobuf field ordinal values. For example, say we define the following Protobuf
 * message:
 *
 * <pre>
 * message MyProtobufType {
 *   optional int32 f1 = 1;
 *   optional int32 f2 = 3;
 *   optional int32 f3 = 7;
 * }
 * </pre>
 *
 * Input Tuples are expected to contain field values in order {@code (f1, f2, f3)}. Tuples may
 * contain fewer values than Protobuf message fields (e.g. only {@code (f1, f2)} in the prior
 * example); Any remaining fields will be left unset.
 *
 * @author Vikram Oberoi
 */
public class PigToProtobuf {
  private static final Logger LOG = LoggerFactory.getLogger(PigToProtobuf.class);

  public PigToProtobuf() {}

  @SuppressWarnings("unchecked")
  public static <M extends Message> M tupleToMessage(Class<M> protoClass, Tuple tuple) {
    Builder builder = Protobufs.getMessageBuilder(protoClass);
    return (M) tupleToMessage(builder, tuple);
  }

  /**
   * Turn a Tuple into a Message with the given type.
   * @param builder a builder for the Message type the tuple will be converted to
   * @param tuple the tuple
   * @return a message representing the given tuple
   */
  public static Message tupleToMessage(Builder builder, Tuple tuple) {
    return tupleToMessage(builder, builder.getDescriptorForType().getFields(), tuple);
  }

  /**
   * @param builder
   * @param fieldDescriptors should be same as builder.getDescriptorForType.getFields().
   *        Avoids overhead of getFields() which creates an array each time.
   * @param tuple
   * @return
   */
  public static Message tupleToMessage(Builder builder, List<FieldDescriptor> fieldDescriptors, Tuple tuple) {
    if (tuple == null) {
      return  builder.build();
    }

    for (int i = 0; i < fieldDescriptors.size() && i < tuple.size(); i++) {
      Object tupleField = null;
      FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);

      try {
        tupleField = tuple.get(i);
      } catch (ExecException e) {
        LOG.warn("Could not convert tuple field " + tupleField + " to field with descriptor " + fieldDescriptor);
        continue;
      }

      if (tupleField != null) {
        try {
          if (fieldDescriptor.isRepeated()) {
            // Repeated fields are set with Lists containing objects of the fields' Java type.
            builder.setField(fieldDescriptor,
                dataBagToRepeatedField(builder, fieldDescriptor, (DataBag) tupleField));
          } else {
            if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
              Builder nestedMessageBuilder = builder.newBuilderForField(fieldDescriptor);
              builder.setField(fieldDescriptor,
                  tupleToMessage(nestedMessageBuilder, (Tuple) tupleField));
            } else {
              builder.setField(fieldDescriptor,
                  tupleFieldToSingleField(fieldDescriptor, tupleField));
            }
          }
        } catch (Exception e) {
          String value = String.valueOf(tupleField);
          final int max_length = 100;
          if (max_length < value.length()) {
            value = value.substring(0, max_length - 3) + "...";
          }
          String type = tupleField == null ? "unknown" : tupleField.getClass().getName();
          throw new RuntimeException(String.format(
              "Failed to set field '%s' using tuple value '%s' of type '%s' at index %d",
              fieldDescriptor.getName(), value, type, i), e);
        }
      }
    }

    return builder.build();
  }

  /**
   * For a given <code>ResourceSchema</code>, generate a protobufs <code>Descriptor</code> with analagous field names
   * and types.
   *
   * @param schema Pig schema.
   * @return Protobufs Descriptor
   * @throws Descriptors.DescriptorValidationException
   */
  public static Descriptor schemaToProtoDescriptor(ResourceSchema schema)
      throws DescriptorValidationException {
      return schemaToProtoDescriptor(schema, null);
  }

  /**
   * For a given <code>ResourceSchema</code>, generate a protobufs <code>Descriptor</code> with analogous field names
   * and types.
   *
   * @param schema Pig schema.
   * @param extraFields optionally pass a List of extra fields (Pairs of name:type) to be included.
   * @return Protobufs Descriptor
   * @throws Descriptors.DescriptorValidationException
   */
  public static Descriptor schemaToProtoDescriptor(ResourceSchema schema, List<Pair<String, Type>> extraFields)
      throws DescriptorValidationException {

    // init protobufs
    DescriptorProto.Builder desBuilder = DescriptorProto.newBuilder();

    int count = 0;
    for (ResourceFieldSchema fieldSchema : schema.getFields()) {
      // Pig types
      int position = ++count;
      String fieldName = fieldSchema.getName();
      byte dataTypeId = fieldSchema.getType();

      // determine and add protobuf types
      Type protoType = pigTypeToProtoType(dataTypeId);
      LOG.info("Mapping Pig field " + fieldName + " of type " + dataTypeId + " to protobuf type: " + protoType);

      addField(desBuilder, fieldName, position, protoType);
    }

    if (count == 0) {
      throw new IllegalArgumentException("ResourceSchema does not have any fields");
    }

    // If extra fields are needed, let's add them
    if (extraFields != null) {
      for (Pair<String, Type> extraField : extraFields) {
        addField(desBuilder, extraField.first, ++count, extraField.second);
      }
    }

    desBuilder.setName("PigToProtobufDynamicBuilder");

    return Protobufs.makeMessageDescriptor(desBuilder.build());
  }

  /**
   * Converts a DataBag into a List of objects with the type in the given FieldDescriptor. DataBags
   * don't map cleanly to repeated protobuf types, so each Tuple has to be unwrapped (by taking the
   * first element if the type is primitive or by converting the Tuple to a Message if the type is
   * MESSAGE), and the contents have to be appended to a List.
   * @param containingMessageBuilder a Message builder for the Message that contains this repeated field
   * @param fieldDescriptor a FieldDescriptor for this repeated field
   * @param bag the DataBag being serialized
   * @return a protobuf-friendly List of fieldDescriptor-type objects
   */
  private static List<Object> dataBagToRepeatedField(Builder containingMessageBuilder, FieldDescriptor fieldDescriptor, DataBag bag) {
    ArrayList<Object> bagContents = new ArrayList<Object>((int)bag.size());
    Iterator<Tuple> bagIter = bag.iterator();

    while (bagIter.hasNext()) {
      Tuple tuple = bagIter.next();
      if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
        Builder nestedMessageBuilder = containingMessageBuilder.newBuilderForField(fieldDescriptor);
        bagContents.add(tupleToMessage((Builder)nestedMessageBuilder, tuple));
      } else {
        try {
          bagContents.add(tupleFieldToSingleField(fieldDescriptor, tuple.get(0)));
        } catch (ExecException e) {
          LOG.warn("Could not add a value for repeated field with descriptor " + fieldDescriptor);
        }
      }
    }

    return bagContents;
  }

  /**
   * Converts a tupleField string to its corresponding protobuf enum type if necessary, otherwise
   * returns the tupleField as is.
   * @param fieldDescriptor the FieldDescriptor for the given tuple field
   * @param tupleField the tupleField being converted to a protobuf field
   * @return the protobuf type for the given tupleField. This will be the tupleField itself unless it's an enum, in which case this will return the enum type for the field.
   */
  private static Object tupleFieldToSingleField(FieldDescriptor fieldDescriptor, Object tupleField) {
    // type convertion should match with ProtobufToPig.getPigScriptDataType
    switch (fieldDescriptor.getType()) {
    case ENUM:
      return toEnumValueDescriptor(fieldDescriptor, (String) tupleField);
    case BOOL:
      return Boolean.valueOf((Integer)tupleField != 0);
    case BYTES:
      return ByteString.copyFrom(((DataByteArray)tupleField).get());
    default:
      return tupleField;
    }
  }

  private static EnumValueDescriptor toEnumValueDescriptor(FieldDescriptor fieldDescriptor,
      String name) {
    EnumValueDescriptor out = fieldDescriptor.getEnumType().findValueByName(name);
    if (out == null) {
      throw new IllegalArgumentException(
          String.format("Failed to convert string '%s'" +
              " to enum value of type '%s'", name,
              fieldDescriptor.getEnumType().getFullName()));
    }
    return out;
  }

  /**
   * Add a field to a protobuf builder
   */
  private static void addField(DescriptorProto.Builder builder, String name, int fieldId, Type type) {
    FieldDescriptorProto.Builder fdBuilder = FieldDescriptorProto.newBuilder()
      .setName(name)
      .setNumber(fieldId)
      .setType(type);
    builder.addField(fdBuilder.build());
  }

  /**
   * For a given Pig type, return the protobufs type that maps to it.
   */
  private static Type pigTypeToProtoType(byte pigTypeId) {

    switch(pigTypeId) {
        case DataType.BOOLEAN:
          return Type.TYPE_BOOL;
        case DataType.INTEGER:
          return Type.TYPE_INT32;
        case DataType.LONG:
          return Type.TYPE_INT64;
        case DataType.FLOAT:
          return Type.TYPE_FLOAT;
        case DataType.DOUBLE:
          return Type.TYPE_DOUBLE;
        case DataType.CHARARRAY:
          return Type.TYPE_STRING;
        case DataType.BYTEARRAY:
          return Type.TYPE_BYTES;
        default:
          throw new IllegalArgumentException("Unsupported Pig type passed (" + pigTypeId +
              ") where a simple type is expected while converting Pig to a dynamic Protobuf");
    }
  }
}
