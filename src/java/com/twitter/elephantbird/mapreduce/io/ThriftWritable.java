package com.twitter.elephantbird.mapreduce.io;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.TypeRef;

/**
 * {@link BinaryWritable} for Thrift 
 */
public class ThriftWritable<M extends TBase<?>> extends BinaryWritable<M> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftWritable.class);
  
  /**
   * Returns a ThriftWritable for a given Thrift class.
   */
  public static <M extends TBase<?>> ThriftWritable<M> newInstance(Class<M> tClass) {
    return new ThriftWritable<M>(new TypeRef<M>(tClass){});
  }
  
  public ThriftWritable(TypeRef<M> typeRef) {
    this(null, typeRef);
  }

  public ThriftWritable(M message, TypeRef<M> typeRef) {
    super(message, new ThriftConverter<M>(typeRef));
    LOG.debug("TProtoWritable, typeClass is " + typeRef.getRawClass());
  }
}
