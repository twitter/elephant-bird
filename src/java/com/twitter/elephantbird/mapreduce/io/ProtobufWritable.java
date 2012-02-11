package com.twitter.elephantbird.mapreduce.io;

import java.util.List;

import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M.
 */

public class ProtobufWritable<M extends Message> extends BinaryWritable<M> {

  private List<GeneratedExtension<M, ?>> protoExtensions;

  public ProtobufWritable() {
    super(null, null);
  }

  public ProtobufWritable(TypeRef<M> typeRef) {
    this(null, typeRef);
  }

  public ProtobufWritable(M message, TypeRef<M> typeRef) {
    super(message, new ProtobufConverter<M>(typeRef));
  }

  public <N extends Message> ProtobufWritable(TypeRef<M> typeRef,
      List<GeneratedExtension<M, ?>> protoExtensions) {
    this(typeRef);
    this.protoExtensions = protoExtensions;
  }


  /**
   * Returns a ProtobufWritable for a given Protobuf class.
   */
  public static <M extends Message> ProtobufWritable<M> newInstance(Class<M> tClass) {
    return new ProtobufWritable<M>(new TypeRef<M>(tClass){});
  }

  public static <M extends Message> ProtobufWritable<M> newInstance(
      Class<M> tClass, List<GeneratedExtension<M, ?>> protoExtensions) {
    return new ProtobufWritable<M>(new TypeRef<M>(tClass){}, protoExtensions);
  }

  @Override
  protected BinaryConverter<M> getConverterFor(Class<M> clazz) {
    if (protoExtensions !=null) {
      return ProtobufConverter.newInstance(clazz, protoExtensions);
    }
    return ProtobufConverter.newInstance(clazz);
  }

  public void setExtensions(List<GeneratedExtension<M, ?>> protoExtensionClasses) {
    this.protoExtensions = protoExtensionClasses;
  }
}
