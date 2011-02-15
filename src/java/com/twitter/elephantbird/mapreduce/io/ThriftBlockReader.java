package com.twitter.elephantbird.mapreduce.io;

import java.io.InputStream;

import com.twitter.elephantbird.util.TypeRef;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class to read blocks of Thrift objects.
 * See the {@link ProtobufBlockReader} for more info.
 */
public class ThriftBlockReader<M extends TBase<?, ?>> extends BinaryBlockReader<M> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftBlockReader.class);

  public ThriftBlockReader(InputStream in, TypeRef<M> typeRef) {
    super(in, new ThriftConverter<M>(typeRef));
    LOG.info("ThriftBlockReader, my typeClass is " + typeRef.getRawClass());
  }
}
