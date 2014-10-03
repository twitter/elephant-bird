package com.twitter.elephantbird.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.Pair;

/**
 * Translates a Thrift object into a Protocol Buffer message.
 * <p>
 * For most data types, there is a 1:1 mapping between Thrift
 * and Protobufs, except for:<ul/>
 *
 * <li> enums: converted to their String representations.
 * <li> lists: added to the enclosing struct as a repeated field.
 * <li> sets: protobufs doesn't have native support for them so they're
 * treated as lists and we rely on thrift to enforce uniqueness.
 * <li> maps: protobufs also doesn't have native support for these so we
 * create an intermediate Message type with key and value fields of the
 * appropriate types and add these as repeated fields of the enclosing
 * struct.<ul/>
 *
 * @param <T> Source thrift class
 */
public class ThriftToDynamicProto<T extends TBase<?, ?>> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftToDynamicProto.class);
  private static final List<Pair<String, Type>> EMPTY_FIELDS = new ArrayList<Pair<String, Type>>();

  private static final String MAP_KEY_FIELD_NAME   = "key";
  private static final String MAP_VALUE_FIELD_NAME = "value";

  private boolean supportNestedObjects = false;
  private boolean ignoreUnsupportedTypes = false;

  private final Descriptors.FileDescriptor fileDescriptor;

  // a map of descriptors keyed to their typeName
  private Map<String, DescriptorProtos.DescriptorProto.Builder> descriptorBuilderMap
      = Maps.newHashMap();

  // a map of message builders keyed to their typeName
  private Map<String, DynamicMessage.Builder> messageBuilderMap = Maps.newHashMap();

  /**
   * Create a Dynamic Protocol Buffer message with fields equivalent to those in the provided
   * Thrift class.
   * @param thriftClass
   * @throws DescriptorValidationException
   */
  public ThriftToDynamicProto(Class<T> thriftClass) throws DescriptorValidationException {
    this(thriftClass, new ArrayList<Pair<String, Type>>());
  }

  public ThriftToDynamicProto(Class<T> thriftClass,
                              boolean supportNestedObjects,
                              boolean ignoreUnsupportedTypes) throws DescriptorValidationException {
    this(thriftClass, new ArrayList<Pair<String, Type>>(), supportNestedObjects,
        ignoreUnsupportedTypes);
  }

  /**
   * Create a converter that produces a Dynamic Protocol Buffer message with fields equivalent to
   * those in the provided Thrift class, plus additional fields as defined by extraFields. Nested
   * Thrift Structs will be ignored, but other unsupported types will throw an exception.
   * @param thriftClass class to use to generate the protobuf schema
   * @param extraFields a list of pairs of (fieldName, protobufType) that defines extra fields to
   * add to the generated message.
   * @throws DescriptorValidationException
   */
  public ThriftToDynamicProto(Class<T> thriftClass, List<Pair<String, Type>> extraFields)
    throws DescriptorValidationException {
    this(thriftClass, extraFields, false, false);
  }

  /**
   * Create a converter that produces a Dynamic Protocol Buffer message with fields equivalent to
   * those in the provided Thrift class, plus additional fields as defined by extraFields.
   * @param thriftClass class to use to generate the protobuf schema
   * @param extraFields a list of pairs of (fieldName, protobufType) that defines extra fields to
   * add to the generated message.
   * @param supportNestedObjects if true, builds a nested structure. Defaults is false, which will
   * silently ignore nested objects, which include structs, lists/sets of structs and maps of all
   * types.  Lists/sets of base types are not considered to be nested objects.
   * @param ignoreUnsupportedTypes if true, ignores types that aren't supported. If false an
   * exception will be thrown when an unsupported type is encountered. Default is false.
   * @throws DescriptorValidationException
   */
  public ThriftToDynamicProto(Class<T> thriftClass,
                              List<Pair<String, Type>> extraFields,
                              boolean supportNestedObjects,
                              boolean ignoreUnsupportedTypes) throws DescriptorValidationException {
    this.supportNestedObjects = supportNestedObjects;
    this.ignoreUnsupportedTypes = ignoreUnsupportedTypes;

    // setup the descriptor proto builder for the top-level class
    DescriptorProtos.DescriptorProto.Builder desBuilder
        = DescriptorProtos.DescriptorProto.newBuilder();
    desBuilder.setName(protoMessageType(thriftClass));
    descriptorBuilderMap.put(desBuilder.getName(), desBuilder);

    // convert the thrift schema to a proto schema
    thriftToProtoSchema(desBuilder, TStructDescriptor.getInstance(thriftClass), extraFields);

    // add all of the message types to the file descriptor proto builder
    DescriptorProtos.FileDescriptorProto.Builder fileDescProtoBuilder =
      DescriptorProtos.FileDescriptorProto.newBuilder();
    for (DescriptorProtos.DescriptorProto.Builder builder : descriptorBuilderMap.values()) {
      fileDescProtoBuilder.addMessageType(builder);
    }

    // create dynamic message builders to be cloned for all types
    Descriptors.FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(
      fileDescProtoBuilder.build(), new Descriptors.FileDescriptor[0]);

    for (String type : descriptorBuilderMap.keySet()) {
      Descriptors.Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName(type);
      messageBuilderMap.put(type, DynamicMessage.newBuilder(msgDescriptor));
    }

    fileDescriptor = dynamicDescriptor;
  }

  /**
   * For the given thriftClass, return a Protobufs builder to build a similar protobuf class.
   * @param thriftClass The thrift class for which the builder is desired.
   * @return a protobuf message builder
   */
  public Message.Builder getBuilder(Class<? extends TBase<?, ?>> thriftClass) {
    return messageBuilderMap.get(protoMessageType(thriftClass)).clone();
  }

  /**
   * Return Protobufs builder for a Map field
   */
  private Message.Builder mapEntryProtoBuilder(TStructDescriptor descriptor, Field field) {
    return messageBuilderMap.get(mapProtoMessageType(descriptor, field)).clone();
  }

  private void thriftToProtoSchema(DescriptorProtos.DescriptorProto.Builder desBuilder,
      TStructDescriptor fieldDesc, List<Pair<String, Type>> extraFields)
      throws DescriptorValidationException {
    int maxThriftId = doSchemaMapping(desBuilder, fieldDesc);

    // handle extra fields if they exist. Only supported on the top level message
    int extraFieldIdx = maxThriftId + 1;
    for (Pair<String, Type> extraField : extraFields) {
      addProtoField(desBuilder,
                    extraField.getFirst(),
                    ++extraFieldIdx,
                    extraField.getSecond(),
                    false);
    }
  }

  private int doSchemaMapping(DescriptorProtos.DescriptorProto.Builder desBuilder,
      TStructDescriptor fieldDesc) throws DescriptorValidationException {
    int maxThriftId = 0;
    for (Field tField : fieldDesc.getFields()) {
      maxThriftId = Math.max(tField.getFieldId(), maxThriftId);

      if (supportNestedObjects && tField.isMap()) {
        String typeName = mapProtoMessageType(fieldDesc, tField);
        if (descriptorBuilderMap.get(typeName) == null) {
          DescriptorProtos.DescriptorProto.Builder mapBuilder =
            mapDescriptorProtoBuilder(tField, typeName);

          descriptorBuilderMap.put(typeName, mapBuilder);
          addProtoField(desBuilder, tField.getName(), tField.getFieldId() + 1, typeName, true);
        }
      } else {
        Field field = resolveField(tField);
        Type protoType = thriftTypeToProtoType(field);
        boolean isContainer = isContainer(tField);

        if (supportNestedObjects && protoType == Type.TYPE_MESSAGE) {
          String typeName = resolveMessageTypeName(field.gettStructDescriptor());

          // Protobuf field ids start at 1. Thrift starts at 0.
          addProtoField(desBuilder,
                        tField.getName(),
                        tField.getFieldId() + 1,
                        typeName,
                        isContainer);
        } else if (protoType != null) {
          if (supportNestedObjects
             || (!supportNestedObjects && !hasNestedObject(tField))) {
            addProtoField(desBuilder,
                          tField.getName(),
                          tField.getFieldId() + 1,
                          protoType,
                          isContainer);
          }
        }
      }
    }

    return maxThriftId;
  }

  // When dealing with a Set/List, we want to retrieve the relevant Field type
  private Field resolveField(Field inputField) {
    if (inputField.isList()) {
      return inputField.getListElemField();
    } else if (inputField.isSet()) {
      return inputField.getSetElemField();
    } else {
      return inputField;
    }
  }

  /**
   * Determines whether a field is considered to be a nested object based on:
   * - whether the field itself is a struct
   * - whether the field is a list/set of structs
   * - whether field is a Map
   */
  private boolean hasNestedObject(Field field) {
    return field.isStruct()
      || (field.isList() && field.getListElemField().isStruct())
      || (field.isSet() && field.getSetElemField().isStruct())
      || field.isMap();
  }

  /**
   * Generate a DescriptorProto.Builder for the Message type that will be used
   * to represent the entries of the input Map field.
   *
   * @param field a Map Field (field.isMap() == true)
   * @param typeName name of new message type
   */
  private DescriptorProtos.DescriptorProto.Builder mapDescriptorProtoBuilder(
    Field field, String typeName) throws DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder mapBuilder =
      DescriptorProtos.DescriptorProto.newBuilder().setName(typeName);

    Field keyField = field.getMapKeyField();
    Field valueField = field.getMapValueField();

    DescriptorProtos.FieldDescriptorProto.Builder keyBuilder = mapKeyProtoBuilder();
    DescriptorProtos.FieldDescriptorProto.Builder valueBuilder = mapValueProtoBuilder();

    setBuilderTypeFromField(keyField, keyBuilder);
    setBuilderTypeFromField(valueField, valueBuilder);

    mapBuilder.addField(keyBuilder.build());
    mapBuilder.addField(valueBuilder.build());

    return mapBuilder;
  }

  // field descriptor for key field to be used in Message used to hold Map entries
  private DescriptorProtos.FieldDescriptorProto.Builder mapKeyProtoBuilder() {
    return fieldDescriptorProtoBuilder(MAP_KEY_FIELD_NAME, 1).setLabel(Label.LABEL_REQUIRED);
  }

  // field descriptor for value field to be used in Message used to hold Map entries
  private DescriptorProtos.FieldDescriptorProto.Builder mapValueProtoBuilder() {
    return fieldDescriptorProtoBuilder(MAP_VALUE_FIELD_NAME, 2).setLabel(Label.LABEL_REQUIRED);
  }

  private void setBuilderTypeFromField(
      Field field,
      DescriptorProtos.FieldDescriptorProto.Builder builder
   ) throws DescriptorValidationException {
    Type valueProtoType = thriftTypeToProtoType(field);
    if (valueProtoType == Type.TYPE_MESSAGE) {
      builder.setTypeName(resolveMessageTypeName(field.gettStructDescriptor()));
    } else if (valueProtoType != null) {
      builder.setType(valueProtoType);
    }
  }

  /**
   * For a TStructDescriptor, resolves the typeName and optionally converts and memoizes it's
   * schema.
   */
  private String resolveMessageTypeName(TStructDescriptor descriptor)
    throws DescriptorValidationException {
    String typeName = protoMessageType(descriptor.getThriftClass());

    // Anytime we have a new message typeName, we make sure that we have a builder for it.
    // If not, we create one.
    DescriptorProtos.DescriptorProto.Builder builder = descriptorBuilderMap.get(typeName);
    if (builder == null) {
      builder = DescriptorProtos.DescriptorProto.newBuilder();
      builder.setName(typeName);
      descriptorBuilderMap.put(typeName, builder);
      doSchemaMapping(builder, descriptor);
    }

    return typeName;
  }

  private void addProtoField(DescriptorProtos.DescriptorProto.Builder builder, String name,
                             int fieldIdx, Type type, boolean isRepeated) {
    DescriptorProtos.FieldDescriptorProto.Builder fdBuilder
        = fieldDescriptorProtoBuilder(name, fieldIdx).setType(type);
    if (isRepeated) {
      fdBuilder.setLabel(Label.LABEL_REPEATED);
    }
    builder.addField(fdBuilder.build());
  }

  private void addProtoField(DescriptorProtos.DescriptorProto.Builder builder, String name,
                             int fieldIdx, String type, boolean isRepeated) {
    DescriptorProtos.FieldDescriptorProto.Builder fdBuilder
        = fieldDescriptorProtoBuilder(name, fieldIdx).setTypeName(type);
    if (isRepeated) {
      fdBuilder.setLabel(Label.LABEL_REPEATED);
    }
    builder.addField(fdBuilder.build());
  }

  private DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorProtoBuilder(
      String name, int fieldIdx) {
    DescriptorProtos.FieldDescriptorProto.Builder fdBuilder =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
      .setName(name)
      .setNumber(fieldIdx);
    return fdBuilder;
  }

  /**
   * Convert a thrift object to a protobuf message.
   * @param thriftObj thrift object
   * @return protobuf protobuf message
   */
  @SuppressWarnings("unchecked")
  public Message convert(T thriftObj) {
    return doConvert((TBase<?, ?>)
                     Preconditions.checkNotNull(thriftObj, "Can not convert a null object"));
  }

  /**
   * conver TBase object to Message object
   * @param thriftObj
   */
  @SuppressWarnings("unchecked")
  public <F extends TFieldIdEnum> Message doConvert(TBase<?, F> thriftObj) {
    if (thriftObj == null) { return null; }

    Class<TBase<?, F>> clazz = (Class<TBase<?, F>>) thriftObj.getClass();
    checkState(clazz);

    Message.Builder builder = getBuilder(clazz);

    TStructDescriptor fieldDesc = TStructDescriptor.getInstance(clazz);
    int fieldId = 0;
    for (Field tField : fieldDesc.getFields()) {
      // don't want to carry over default values from unset fields
      if (!thriftObj.isSet((F) tField.getFieldIdEnum())
          || (!supportNestedObjects && hasNestedObject(tField))) {
        fieldId++;
        continue;
      }

      // recurse into the object if it's a struct, otherwise just add the field
      if (supportNestedObjects && tField.getType() == TType.STRUCT) {
        TBase<?, ?> fieldValue = (TBase<?, ?>) fieldDesc.getFieldValue(fieldId++, thriftObj);
        Message message = doConvert(fieldValue);
        if (message != null) {
          FieldDescriptor protoFieldDesc = builder.getDescriptorForType().findFieldByName(
              tField.getName());
          builder.setField(protoFieldDesc, message);
        }
      } else {
        fieldId = convertField(thriftObj, builder, fieldDesc, fieldId, tField);
      }
    }
    return builder.build();
  }

  private void checkState(Class<? extends TBase<?, ?>> thriftClass) {
    Preconditions.checkState(hasBuilder(thriftClass),
      "No message builder found for thrift class: " + thriftClass.getCanonicalName());
  }

  private boolean hasBuilder(Class<? extends TBase<?, ?>> thriftClass) {
    return messageBuilderMap.get(protoMessageType(thriftClass)) != null;
  }

  private Object sanitizeRawValue(Object value, Field tField) {
    Object returnValue = value;

    if (tField.isEnum()) {
      // TODO: proper enum handling
      returnValue = returnValue.toString();
    } else if (tField.isBuffer()) {
      returnValue = ByteString.copyFrom((byte[]) returnValue);
    }

    if (returnValue instanceof Byte) {
      returnValue = new Integer((Byte) returnValue);
    } else if (returnValue instanceof Short) {
      returnValue = new Integer((Short) returnValue);
    }

    return returnValue;
  }

  /*
   * Determines if the field in question is a Set or List of a Struct type
   */
  private boolean isStructContainer(Field tField) {
    return (tField.isList() && tField.getListElemField().isStruct())
      || (tField.isSet() && tField.getSetElemField().isStruct());
  }

  @SuppressWarnings("unchecked")
  private int convertField(TBase<?, ?> thriftObj, Message.Builder builder,
                           TStructDescriptor fieldDesc, int fieldId, Field tField) {
    int tmpFieldId = fieldId;
    FieldDescriptor protoFieldDesc = builder.getDescriptorForType().findFieldByName(
        tField.getName());

    if (protoFieldDesc == null) {
      // not finding a field might be ok if we're ignoring an unsupported types
      Type protoType = thriftTypeToProtoType(tField);
      if (protoType == null
          && (ignoreUnsupportedTypes
              || (!supportNestedObjects && hasNestedObject(tField)))) {
        return tmpFieldId; // no-op
      }
      throw new RuntimeException("Field " + tField.getName() + " not found in dynamic protobuf.");
    }

    Object fieldValue = fieldDesc.getFieldValue(tmpFieldId++, thriftObj);
    if (fieldValue == null) {
      return tmpFieldId;
    }

    try {
      // For non-Map containers that contain struct types,
      // we have to convert each struct into a Message.
      if (isStructContainer(tField)) {
        List<Message> convertedStructs = Lists.newLinkedList();

        Iterable<TBase<?, ?>> structIterator = (Iterable<TBase<?, ?>>) fieldValue;
        for (TBase<?, ?> struct : structIterator) {
          convertedStructs.add(doConvert(struct));
        }

        fieldValue = convertedStructs;
      } else if (tField.isMap()) {
        List<Message> convertedMapEntries = Lists.newLinkedList();

        Map<?, ?> rawMap = (Map) fieldValue;
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
          Message.Builder mapBuilder = mapEntryProtoBuilder(fieldDesc, tField);
          Message msg = buildMapEntryMessage(mapBuilder, tField, entry.getKey(), entry.getValue());
          convertedMapEntries.add(msg);
        }

        fieldValue = convertedMapEntries;
      } else {
        // protobufs throws an exception if you try to set byte on an int32 field so we need to
        // convert it to an Integer before it gets set
        fieldValue = sanitizeRawValue(fieldValue, tField);
      }

      // Container types have to be added as repeated fields
      if (isContainer(tField)) {
        Iterable<?> container = (Iterable) fieldValue;
        for (Object obj : container) {
          builder.addRepeatedField(protoFieldDesc, obj);
        }
      } else {
        builder.setField(protoFieldDesc, fieldValue);
      }
    } catch (IllegalArgumentException e) {
      LOG.error(String.format("Could not set protoField(index=%d, name=%s, type=%s) with "
        + "thriftField(index=%d, name=%s, type=%d, value=%s)",
        protoFieldDesc.getIndex(), protoFieldDesc.getName(), protoFieldDesc.getType(),
          tmpFieldId - 1, tField.getName(), tField.getType(), fieldValue), e);
      throw e;
    }

    return tmpFieldId;
  }

  /**
   * Builds a Message that contains the key value pair of a Map entry
   */
  private Message buildMapEntryMessage(Message.Builder mapBuilder, Field field,
                                       Object mapKey, Object mapValue) {
    FieldDescriptor keyFieldDescriptor =
      mapBuilder.getDescriptorForType().findFieldByName(MAP_KEY_FIELD_NAME);
    FieldDescriptor valueFieldDescriptor =
      mapBuilder.getDescriptorForType().findFieldByName(MAP_VALUE_FIELD_NAME);

    boolean isKeyStruct = field.getMapKeyField().isStruct();
    boolean isValueStruct = field.getMapValueField().isStruct();

    Object convertedKey;
    if (isKeyStruct) {
      convertedKey = doConvert((TBase<?, ?>) mapKey);
    } else {
      convertedKey = sanitizeRawValue(mapKey, field.getMapKeyField());
    }

    Object convertedValue;
    if (isValueStruct) {
      convertedValue = doConvert((TBase<?, ?>) mapValue);
    } else {
      convertedValue = sanitizeRawValue(mapValue, field.getMapValueField());
    }

    mapBuilder.setField(keyFieldDescriptor, convertedKey);
    mapBuilder.setField(valueFieldDescriptor, convertedValue);

    return mapBuilder.build();
  }

  // Checks a field to determine whether it's a List/Set/Map
  private boolean isContainer(Field field) {
    return field.isSet() || field.isList() || field.isMap();
  }

  private Type thriftTypeToProtoType(Field tField) {
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
      case TType.STRUCT:
        if (supportNestedObjects) { return Type.TYPE_MESSAGE; }
        return null;
      case TType.MAP:
        return null;
      case TType.SET:
        return null;
      case TType.LIST:
        return null;
      default:
        if (ignoreUnsupportedTypes) {
          LOG.warn("Thrift type " + thriftType + " not supported for field "
             + tField.getName() + ". Ignoring");
          return null;
        }
        throw new IllegalArgumentException("Can't map Thrift type " + thriftType
          + " to a Protobuf type for field: " + tField.getName());
    }
  }

  // name the proto message type after the thrift class name. Dots are not permitted in protobuf
  // names
  private String protoMessageType(Class<? extends TBase<?, ?>> thriftClass) {
    return thriftClass.getCanonicalName().replace(".", "_");
  }

  /**
   * name the proto message used for Map types after the thrift class name of the enclosing
   * struct and the field name
   */
  private String mapProtoMessageType(TStructDescriptor descriptor, Field field) {
    return String.format("%s_%s", protoMessageType(descriptor.getThriftClass()), field.getName());
  }

  // Given the class name, finds the corresponding Descriptor and return the appropriate
  // FieldDescriptor
  public FieldDescriptor getFieldDescriptor(Class<? extends TBase<?, ?>> thriftClass,
                                            String fieldName) {
    checkState(thriftClass);
    Descriptors.Descriptor descriptor = getBuilder(thriftClass).getDescriptorForType();
    return descriptor.findFieldByName(fieldName);
  }

  // Picks off the FileDescriptor for this instance
  public Descriptors.FileDescriptor getFileDescriptor() {
    return fileDescriptor;
  }
}
