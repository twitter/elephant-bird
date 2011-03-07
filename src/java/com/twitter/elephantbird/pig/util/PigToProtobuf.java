package com.twitter.elephantbird.pig.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.util.Protobufs;

/**
 * A class for turning Pig Tuples into codegen'd protos for custom Pig StoreFuncs.
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
    List<FieldDescriptor> fieldDescriptors = builder.getDescriptorForType().getFields();

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
        if (fieldDescriptor.isRepeated()) {
          // Repeated fields are set with Lists containing objects of the fields' Java type.
          builder.setField(fieldDescriptor, dataBagToRepeatedField(builder, fieldDescriptor, (DataBag)tupleField));
        } else {
          if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
            Builder nestedMessageBuilder = builder.newBuilderForField(fieldDescriptor);
            builder.setField(fieldDescriptor, tupleToMessage((Builder)nestedMessageBuilder, (Tuple)tupleField));
          } else {
            builder.setField(fieldDescriptor, tupleFieldToSingleField(fieldDescriptor, tupleField));
          }
        }
      }
    }

    return builder.build();
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
      // Convert tupleField to the enum value.
      return fieldDescriptor.getEnumType().findValueByName((String)tupleField);
    case BOOL:
      return Boolean.valueOf((Integer)tupleField != 0);
    case BYTES:
      return ByteString.copyFrom(((DataByteArray)tupleField).get());
    default:
      return tupleField;
    }
  }
}
