package com.twitter.elephantbird.mapreduce.io;

import java.io.InputStream;

import com.twitter.elephantbird.util.TypeRef;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class to read blocks of Generic objects.
 * See the {@link ProtobufBlockReader} for more info.
 */
public class GenericBlockReader<M> extends BinaryBlockReader<M> {
  private static final Logger LOG = LoggerFactory.getLogger(GenericBlockReader.class);

  public GenericBlockReader(InputStream in, TypeRef<M> typeRef, BinaryConverter<M> binaryConverter) {
    super(in, binaryConverter);
    LOG.info("GenericBlockReader, my typeClass is " + typeRef.getRawClass());
  }
}
