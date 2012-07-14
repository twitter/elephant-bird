package com.twitter.elephantbird.util;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.UninitializedMessageException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Protobufs {
  private static final Logger LOG = LoggerFactory.getLogger(Protobufs.class);

  public static final byte[] KNOWN_GOOD_POSITION_MARKER = new byte[] { 0x29, (byte)0xd8, (byte)0xd5, 0x06, 0x58,
                                                                         (byte)0xcd, 0x4c, 0x29, (byte)0xb2,
                                                                         (byte)0xbc, 0x57, (byte)0x99, 0x21, 0x71,
                                                                         (byte)0xbd, (byte)0xff };

  public static final byte NEWLINE_UTF8_BYTE = '\n';
  public static final byte[] NEWLINE_UTF8_BYTES = new byte[]{NEWLINE_UTF8_BYTE};

  public static final String IGNORE_KEY = "IGNORE";

  private static final String CLASS_CONF_PREFIX = "elephantbird.protobuf.class.for.";

  /**
   * Returns Protobuf class. The class name could be either normal name or
   * its canonical name (canonical name does not have a $ for inner classes).
   */
  public static Class<? extends Message> getProtobufClass(String protoClassName) {
    return getProtobufClass(null, protoClassName);
  }

  private static Class<? extends Message> getProtobufClass(Configuration conf, String protoClassName) {
    // Try both normal name and canonical name of the class.
    Class<?> protoClass = null;
    try {
      if (conf == null) {
        protoClass = Class.forName(protoClassName);
      } else {
        protoClass = conf.getClassByName(protoClassName);
      }
    } catch (ClassNotFoundException e) {
      // the class name might be canonical name.
      protoClass = getInnerProtobufClass(protoClassName);
    }

    return protoClass.asSubclass(Message.class);
  }

  /**
   * For a configured protoClass, should the message be dynamic or is it a pre-generated Message class? If protoClass is
   * null or set to DynamicMessage.class, then the configurer intends for a dynamically generated protobuf to be used.
   */
  public static boolean useDynamicProtoMessage(Class<?> protoClass) {
    return protoClass == null || protoClass.getCanonicalName().equals(DynamicMessage.class.getCanonicalName());
  }

  public static Class<? extends Message> getInnerProtobufClass(String canonicalClassName) {
    // is an inner class and is not visible from the outside.  We have to instantiate
    String parentClass = canonicalClassName.substring(0, canonicalClassName.lastIndexOf("."));
    String subclass = canonicalClassName.substring(canonicalClassName.lastIndexOf(".") + 1);
    return getInnerClass(parentClass, subclass);
  }

  public static Class<? extends Message> getInnerClass(String canonicalParentName, String subclassName) {
    try {
      Class<?> outerClass = Class.forName(canonicalParentName);
      for (Class<?> innerClass: outerClass.getDeclaredClasses()) {
        if (innerClass.getSimpleName().equals(subclassName)) {
          return innerClass.asSubclass(Message.class);
        }
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Could not find class with parent " + canonicalParentName + " and inner class " + subclassName, e);
      throw new IllegalArgumentException(e);
    }
    return null;
  }

  public static Message.Builder getMessageBuilder(Class<? extends Message> protoClass) {
    try {
      Method newBuilder = protoClass.getMethod("newBuilder", new Class[] {});
      return (Message.Builder) newBuilder.invoke(null, new Object[] {});
    } catch (NoSuchMethodException e) {
      LOG.error("Could not find method newBuilder in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Could not access method newBuilder in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    } catch (InvocationTargetException e) {
      LOG.error("Error invoking method newBuilder in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    }
  }

  public static Descriptor getMessageDescriptor(Class<? extends Message> protoClass) {
    try {
      Method getDescriptor = protoClass.getMethod("getDescriptor", new Class[] {});
      return (Descriptor)getDescriptor.invoke(null, new Object[] {});
    } catch (NoSuchMethodException e) {
      LOG.error("Could not find method getDescriptor in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Could not access method getDescriptor in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    } catch (InvocationTargetException e) {
      LOG.error("Error invoking method getDescriptor in class " + protoClass, e);
    }

    return null;
  }

  public static List<String> getMessageFieldNames(Class<? extends Message> protoClass) {
    return Lists.transform(getMessageDescriptor(protoClass).getFields(), new Function<FieldDescriptor, String>() {
      public String apply(FieldDescriptor f) {
        return f.getName();
      }
    });
  }

  /**
   * Returns a Message {@link Descriptor} for a dynamically generated
   * DescriptorProto.
   *
   * @param descProto
   * @throws DescriptorValidationException
   */
  public static Descriptor makeMessageDescriptor(DescriptorProto descProto)
                                      throws DescriptorValidationException {

    DescriptorProtos.FileDescriptorProto fileDescP =
      DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descProto).build();

    Descriptors.FileDescriptor[] fileDescs = new Descriptors.FileDescriptor[0];
    Descriptors.FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, fileDescs);
    return dynamicDescriptor.findMessageTypeByName(descProto.getName());
  }

   // Translates from protobuf field names to other field names, stripping out any that have value IGNORE.
   // i.e. if you have a protobuf with field names a, b, c and your fieldNameTranslations map looks like
   // { "a" => "A", "c" => "IGNORE" }, then you'll end up with A, b
  public static List<String> getMessageFieldNames(Class<? extends Message> protoClass, Map<String, String> fieldNameTranslations) {
    return getMessageFieldNames(getMessageDescriptor(protoClass), fieldNameTranslations);
  }

   public static List<String> getMessageFieldNames(Descriptor descriptor, Map<String, String> fieldNameTranslations) {
     Function<FieldDescriptor, String> fieldTransformer = getFieldTransformerFor(fieldNameTranslations);
     return ListHelper.filter(Lists.transform(descriptor.getFields(), fieldTransformer), Predicates.<String>notNull());
   }

   public static Function<FieldDescriptor, String> getFieldTransformerFor(final Map<String, String> fieldNameTranslations) {
     return new Function<FieldDescriptor, String>() {
       public String apply(FieldDescriptor f) {
         String name = f.getName();
         if (fieldNameTranslations != null && fieldNameTranslations.containsKey(name)) {
           name = fieldNameTranslations.get(name);
         }
         if (IGNORE_KEY.equals(name)) {
           name = null;
         }
         return name;
       }
     };
   }


  @SuppressWarnings("unchecked")
  public static <M extends Message> M parseFrom(Class<M> protoClass, byte[] messageBytes) {
    try {
      Method parseFrom = protoClass.getMethod("parseFrom", new Class[] { byte[].class });
      return (M)parseFrom.invoke(null, new Object[] { messageBytes });
    } catch (NoSuchMethodException e) {
      LOG.error("Could not find method parseFrom in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Could not access method parseFrom in class " + protoClass, e);
      throw new IllegalArgumentException(e);
    } catch (InvocationTargetException e) {
      LOG.error("Error invoking method parseFrom in class " + protoClass, e);
    }

    return null;
  }

  public static DynamicMessage parseDynamicFrom(Class<? extends Message> protoClass, byte[] messageBytes) {
    try {
      return DynamicMessage.parseFrom(getMessageDescriptor(protoClass), messageBytes);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Protocol buffer parsing error in parseDynamicFrom", e);
    } catch(UninitializedMessageException ume) {
      LOG.error("Uninitialized Message error in parseDynamicFrom " + protoClass.getName(), ume);
    }

    return null;
  }

  public static Message instantiateFromClassName(String canonicalClassName) {
    Class<? extends Message> protoClass = getInnerProtobufClass(canonicalClassName);
    Message.Builder builder = getMessageBuilder(protoClass);
    return builder.build();
  }

  public static <M extends Message> M instantiateFromClass(Class<M> protoClass) {
    return Protobufs.parseFrom(protoClass, new byte[] {});
  }

  public static Message addField(Message m, String name, Object value) {
    Message.Builder builder = m.toBuilder();
    setFieldByName(builder, name, value);
    return builder.build();
  }

  public static void setFieldByName(Message.Builder builder, String name, Object value) {
    FieldDescriptor fieldDescriptor = builder.getDescriptorForType().findFieldByName(name);
    if (value == null) {
      builder.clearField(fieldDescriptor);
    } else {
      builder.setField(fieldDescriptor, value);
    }
  }

  public static boolean isFieldSetByName(Message message, String name) {
    return message.hasField(message.getDescriptorForType().findFieldByName(name));
  }

  public static Object getFieldByName(Message message, String name) {
    return message.getField(message.getDescriptorForType().findFieldByName(name));
  }

  public static boolean hasFieldByName(Message message, String name) {
    return message.getDescriptorForType().findFieldByName(name) != null;
  }

  public static Type getTypeByName(Message message, String name) {
    return message.getDescriptorForType().findFieldByName(name).getType();
  }

  /**
   * Returns typeref for a Protobuf class
   */
  public static<M extends Message> TypeRef<M> getTypeRef(String protoClassName) {
    return new TypeRef<M>(getProtobufClass(protoClassName)){};
  }

  /**
   * Returns TypeRef for the Protobuf class that was set using setClass(jobConf);
   */
  public static<M extends Message> TypeRef<M> getTypeRef(Configuration jobConf, Class<?> genericClass) {
    String className = jobConf.get(CLASS_CONF_PREFIX + genericClass.getName());
    if (className == null) {
      throw new RuntimeException(CLASS_CONF_PREFIX + genericClass.getName() + " is not set");
    }
    return new TypeRef<M>(getProtobufClass(jobConf, className)){};
  }

  public static void setClassConf(Configuration jobConf, Class<?> genericClass,
      Class<? extends Message> protoClass) {
    HadoopUtils.setClassConf(jobConf,
           CLASS_CONF_PREFIX + genericClass.getName(),
           protoClass);
  }

  public static Text toText(Message message) {
    return new Text(message.toByteArray());
  }

  public static <B extends Message.Builder> B mergeFromText(B builder, Text bytes)
                                    throws InvalidProtocolBufferException {
    @SuppressWarnings("unchecked")
    B b = (B) builder.mergeFrom(bytes.getBytes());
    return b;
  }

  /**
   * Serializes a single field. All the native fields are serialized using
   * "NoTag" methods in {@link CodedOutputStream}
   * e.g. <code>writeInt32NoTag()</code>. The field index is not written.
   *
   * @param output
   * @param fd
   * @param value
   * @throws IOException
   */
  public static void writeFieldNoTag(CodedOutputStream   output,
                                     FieldDescriptor     fd,
                                     Object              value)
                                     throws IOException {
    if (value == null) {
      return;
    }

    if (fd.isRepeated()) {
      @SuppressWarnings("unchecked")
      List<Object> values = (List<Object>) value;
      for(Object obj : values) {
        writeSingleFieldNoTag(output, fd, obj);
      }
    } else {
      writeSingleFieldNoTag(output, fd, value);
    }
  }

  private static void writeSingleFieldNoTag(CodedOutputStream   output,
                                            FieldDescriptor     fd,
                                            Object              value)
                                            throws IOException {
    switch (fd.getType()) {
    case DOUBLE:
      output.writeDoubleNoTag((Double) value);    break;
    case FLOAT:
      output.writeFloatNoTag((Float) value);      break;
    case INT64:
    case UINT64:
      output.writeInt64NoTag((Long) value);       break;
    case INT32:
      output.writeInt32NoTag((Integer) value);    break;
    case FIXED64:
      output.writeFixed64NoTag((Long) value);     break;
    case FIXED32:
      output.writeFixed32NoTag((Integer) value);  break;
    case BOOL:
      output.writeBoolNoTag((Boolean) value);     break;
    case STRING:
      output.writeStringNoTag((String) value);    break;
    case GROUP:
    case MESSAGE:
      output.writeMessageNoTag((Message) value);  break;
    case BYTES:
      output.writeBytesNoTag((ByteString) value); break;
    case UINT32:
      output.writeUInt32NoTag((Integer) value);   break;
    case ENUM:
      output.writeEnumNoTag(((ProtocolMessageEnum) value).getNumber()); break;
    case SFIXED32:
      output.writeSFixed32NoTag((Integer) value); break;
    case SFIXED64:
      output.writeSFixed64NoTag((Long) value);    break;
    case SINT32:
      output.writeSInt32NoTag((Integer) value);   break;
    case SINT64:
      output.writeSInt64NoTag((Integer) value);   break;

    default:
      throw new IllegalArgumentException("Unknown type " + fd.getType()
                                         + " for " + fd.getFullName());
    }
  }

  /**
   *  Wrapper around {@link #readFieldNoTag(CodedInputStream, FieldDescriptor, Builder)}. <br>
   *  same as <br> <code>
   *  builder.setField(fd, readFieldNoTag(input, fd, builder));  </code>
   */
  public static void setFieldValue(CodedInputStream input,
                              FieldDescriptor  fd,
                              Builder          builder)
                              throws IOException {
    builder.setField(fd, readFieldNoTag(input, fd, builder));
  }

  /**
   * Deserializer for protobuf objects written with
   * {@link #writeFieldNoTag(CodedOutputStream, FieldDescriptor, Object)}.
   *
   * @param input
   * @param fd
   * @param enclosingBuilder required to create a builder when the field
   *                         is a message
   * @throws IOException
   */
  public static Object readFieldNoTag(CodedInputStream input,
                                      FieldDescriptor  fd,
                                      Builder          enclosingBuilder)
                                      throws IOException {
    if (!fd.isRepeated()) {
      return readSingleFieldNoTag(input, fd, enclosingBuilder);
    }

    // repeated field

    List<Object> values = Lists.newArrayList();
    while (!input.isAtEnd()) {
      values.add(readSingleFieldNoTag(input, fd, enclosingBuilder));
    }
    return values;
  }

  private static Object readSingleFieldNoTag(CodedInputStream input,
                                             FieldDescriptor  fd,
                                             Builder          enclosingBuilder)
                                             throws IOException {
    switch (fd.getType()) {
    case DOUBLE:
      return input.readDouble();
    case FLOAT:
      return input.readFloat();
    case INT64:
    case UINT64:
      return input.readInt64();
    case INT32:
      return input.readInt32();
    case FIXED64:
      return input.readFixed64();
    case FIXED32:
      return input.readFixed32();
    case BOOL:
      return input.readBool();
    case STRING:
      return input.readString();
    case GROUP:
    case MESSAGE:
      Builder fieldBuilder = enclosingBuilder.newBuilderForField(fd);
      input.readMessage(fieldBuilder, null);
      return fieldBuilder.build();
    case BYTES:
      return input.readBytes();
    case UINT32:
      return input.readUInt32();
    case ENUM:
      EnumValueDescriptor eVal = fd.getEnumType().findValueByNumber(input.readEnum());
      // ideally if a given enum does not exist, we should search
      // unknown fields. but we don't have access to that here. return default
      return eVal != null ? eVal : fd.getDefaultValue();
    case SFIXED32:
      return input.readSFixed32();
    case SFIXED64:
      return input.readSFixed64();
    case SINT32:
      return input.readSInt32();
    case SINT64:
      return input.readSInt64();

    default:
      throw new IllegalArgumentException("Unknown type " + fd.getType()
          + " for " + fd.getFullName());
    }
  }

}

