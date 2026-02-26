package com.twitter.elephantbird.mapreduce.io;

/**
 * A {@link BinaryWritable} that returns the raw bytes.
 */
public class RawBytesWritable extends BinaryWritable<byte[]> {

  public RawBytesWritable() {
    super(null, new IdentityBinaryConverter());
  }

  @Override
  protected BinaryConverter<byte[]> getConverterFor(Class<byte[]> clazz) {
    return null; // not expected to be invoked since converter is always set.
  }
}

