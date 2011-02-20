package com.twitter.elephantbird.mapreduce.io;

import java.util.Arrays;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M.
 */

public class ProtobufWritable<M extends Message> extends BinaryWritable<M> {

  public ProtobufWritable() {
    super(null, null);
  }

  public ProtobufWritable(TypeRef<M> typeRef) {
    this(null, typeRef);
  }

  public ProtobufWritable(M message, TypeRef<M> typeRef) {
    super(message, new ProtobufConverter<M>(typeRef));
  }

  /**
   * Returns a ProtobufWritable for a given Protobuf class.
   */
  public static <M extends Message> ProtobufWritable<M> newInstance(Class<M> tClass) {
    return new ProtobufWritable<M>(new TypeRef<M>(tClass){});
  }

  @Override
  protected BinaryConverter<M> getConverterFor(Class<M> clazz) {
    return ProtobufConverter.newInstance(clazz);
  }

  /**
   * <p>Returns a hashCode that is stable across multiple instances of JVMs.
   * (<code>hashCode()</code> is not required to return the same value in
   * different instances of the same applications in Java, just in a
   * single instance of the application; Hadoop imposes a more strict requirement.)
   */
  @Override
  public int hashCode() {
    byte[] bytes = serialize();
    return (bytes == null) ? 31 : Arrays.hashCode(bytes);
  }
}
