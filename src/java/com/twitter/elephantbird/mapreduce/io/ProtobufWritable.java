package com.twitter.elephantbird.mapreduce.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M.
 */

public class ProtobufWritable<M extends Message> extends BinaryWritable<M> {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWritable.class);

  public ProtobufWritable(TypeRef<M> typeRef) {
    this(null, typeRef);
  }

  public ProtobufWritable(M message, TypeRef<M> typeRef) {
    super(message, new ProtobufConverter<M>(typeRef));
    LOG.debug("ProtobufWritable, typeClass is " + typeRef.getRawClass());
  }

  /**
   * Returns a ThriftWritable for a given Thrift class.
   */
  public static <M extends Message> ProtobufWritable<M> newInstance(Class<M> tClass) {
    return new ProtobufWritable<M>(new TypeRef<M>(tClass){});
  }

}
