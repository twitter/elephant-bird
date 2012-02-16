package com.twitter.elephantbird.mapreduce.io;

import com.google.protobuf.Message;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M.
 */

public class ProtobufWritable<M extends Message> extends BinaryWritable<M> {

  private ProtobufExtensionRegistry extensionRegistry;

  public ProtobufWritable() {
    this(null, null, null);
  }

  public ProtobufWritable(TypeRef<M> typeRef) {
    this(null, typeRef, null);
  }

  public ProtobufWritable(M message, TypeRef<M> typeRef) {
    this(message, typeRef, null);
  }


  public ProtobufWritable(TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    this(null, typeRef, extensionRegistry);
  }

  public ProtobufWritable(M message, TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    super(message, new ProtobufConverter<M>(typeRef, extensionRegistry));
    this.extensionRegistry = extensionRegistry;
  }


  /**
   * Returns a ProtobufWritable for a given Protobuf class.
   */
  public static <M extends Message> ProtobufWritable<M> newInstance(Class<M> tClass) {
    return new ProtobufWritable<M>(new TypeRef<M>(tClass){});
  }

  public static <M extends Message> ProtobufWritable<M> newInstance(
      Class<M> tClass, ProtobufExtensionRegistry extensionRegistry) {
    return new ProtobufWritable<M>(new TypeRef<M>(tClass){}, extensionRegistry);
  }

  public static <M extends Message> ProtobufWritable<M> newInstance(TypeRef<M> typeRef) {
    return new ProtobufWritable<M>(typeRef);
  }

  public static <M extends Message> ProtobufWritable<M> newInstance(
      TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    return new ProtobufWritable<M>(typeRef, extensionRegistry);
  }

  @Override
  protected BinaryConverter<M> getConverterFor(Class<M> clazz) {
    if (extensionRegistry !=null) {
      return ProtobufConverter.newInstance(clazz, extensionRegistry);
    }
    return ProtobufConverter.newInstance(clazz);
  }

  public void setExtensionRegistry(ProtobufExtensionRegistry extensionRegistry) {
    this.extensionRegistry = extensionRegistry;
  }
}
