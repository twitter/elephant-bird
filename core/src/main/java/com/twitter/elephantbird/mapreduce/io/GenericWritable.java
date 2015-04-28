package com.twitter.elephantbird.mapreduce.io;

/**
 * {@link BinaryWritable} for Generics
 */
public class GenericWritable<M> extends BinaryWritable<M> {
  public GenericWritable(BinaryConverter<M> converter) {
    this(null, converter);
  }

  public GenericWritable(M message, BinaryConverter<M> converter) {
    super(message, converter);
  }

  @Override
  protected BinaryConverter<M> getConverterFor(Class<M> clazz) {
    throw new UnsupportedOperationException();
  }
}
