package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.impl.PigContext;
import org.apache.thrift.TBase;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

public class PigUtil {
  /**
   *  Returns class using Pig's class loader.
   */
  public static Class<?> getClass(String className) {
    try {
      return PigContext.resolveClassName(className);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate " + className, e);
    }
  }

  /**
   * Temporary hack to check if PIG version is 0.9.0 or newer
   */
  public static final boolean Pig9orNewer;
  static {
    boolean methodFound = false;
    try {
      // check for a new method in a class common to both Pig 0.8 and 0.9
      Class<?> cls = Class.forName("org.apache.pig.EvalFunc");
      methodFound = cls.getMethod("getCacheFiles") != null;
    } catch (Exception e) {
    }
    Pig9orNewer = methodFound;
  }

  /**
   * Returns class using Pig's class loader.
   * If that fails tries {@link Protobufs#getProtobufClass(String)} to
   * resolve the class
   *
   */
  public static Class<? extends Message> getProtobufClass(String protoClassName) {
    try {
      // try Pig's loader
      Class<?> protoClass = getClass(protoClassName);
      return protoClass.asSubclass(Message.class);
    } catch (RuntimeException e) {
      // try normal loader first (handles inner class).
      return Protobufs.getProtobufClass(protoClassName);
    }
  }

  /**
   * Invokes the static {@code getDescriptor} method of the given {@link Message} class.
   *
   * @param protoClass the {@link Message} class whose {@link Descriptor} should be retrieved.
   * @return the Descriptor instance for the given {@link Message} class.
   * @throws RuntimeException on any exception encountered during introspection of protoClass or
   * invocation of its static {@code getDescriptor} method.
   * @see Descriptors
   */
  public static Descriptor getProtobufDescriptor(Class<? extends Message> protoClass) {
    try {
      return (Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get Descriptor for Message type '%s'", protoClass.getName()), e);
    }
  }

  /**
   * @param protoClassName name of the {@link Message} class whose {@link Descriptor} should be
   * retrieved.
   * @return the Descriptor instance for the given {@link Message} class.
   * @throws RuntimeException on any exception encountered during class load or introspection of
   * protoClassName or invocation of its static {@code getDescriptor} method.
   * @see #getProtobufDescriptor(Class)
   */
  public static Descriptor getProtobufDescriptor(String protoClassName) {
    return getProtobufDescriptor(getProtobufClass(protoClassName));
  }

  public static<M extends Message> TypeRef<M> getProtobufTypeRef(String protoClassName) {
    return new TypeRef<M>(getProtobufClass(protoClassName)){};
  }

  /** Returns TypeRef using Pig class loader. */
  public static<T extends TBase<?,?>> TypeRef<T> getThriftTypeRef(String thriftClassName) {
    return ThriftUtils.getTypeRef(getClass(thriftClassName));
  }
}
