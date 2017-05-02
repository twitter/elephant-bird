package com.twitter.elephantbird.mapreduce.io;

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
}
