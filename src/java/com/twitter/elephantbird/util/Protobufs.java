package com.twitter.elephantbird.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Protobufs {
  private static final Logger LOG = LoggerFactory.getLogger(Protobufs.class);

  public static final byte[] KNOWN_GOOD_POSITION_MARKER = new byte[] { 0x29, (byte)0xd8, (byte)0xd5, 0x06, 0x58,
                                                                         (byte)0xcd, 0x4c, 0x29, (byte)0xb2,
                                                                         (byte)0xbc, 0x57, (byte)0x99, 0x21, 0x71,
                                                                         (byte)0xbd, (byte)0xff };

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

   // Translates from protobuf field names to other field names, stripping out any that have value IGNORE.
   // i.e. if you have a protobuf with field names a, b, c and your fieldNameTranslations map looks like
   // { "a" => "A", "c" => "IGNORE" }, then you'll end up with A, b
   public static List<String> getMessageFieldNames(Class<? extends Message> protoClass, Map<String, String> fieldNameTranslations) {
     Function<FieldDescriptor, String> fieldTransformer = getFieldTransformerFor(fieldNameTranslations);
     return ListHelper.filter(Lists.transform(getMessageDescriptor(protoClass).getFields(), fieldTransformer), Predicates.<String>notNull());
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
    jobConf.set(CLASS_CONF_PREFIX + genericClass.getName(), protoClass.getName());
  }
}
