package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.impl.PigContext;
import org.apache.thrift.TBase;

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

  public static<M extends Message> TypeRef<M> getProtobufTypeRef(String protoClassName) {
    return new TypeRef<M>(getProtobufClass(protoClassName)){};
  }

  /** Returns TypeRef using Pig class loader. */
  public static<T extends TBase<?,?>> TypeRef<T> getThriftTypeRef(String thriftClassName) {
    return ThriftUtils.getTypeRef(getClass(thriftClassName));
  }
}
