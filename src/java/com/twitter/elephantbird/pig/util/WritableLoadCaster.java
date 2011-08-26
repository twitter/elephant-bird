package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.pig.LoadCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Base LoadCaster implementation which simplifies conversion specification from Writable type to
 * Pig types.
 *
 * @author Andy Schlaikjer
 */
public abstract class WritableLoadCaster<W extends Writable> implements LoadCaster {
  private final DataInputBuffer buf = new DataInputBuffer();
  protected W writable;

  /**
   * Deserializes raw bytes into a Writable instance, returning that instance. We rely on derived
   * classes to initialize {@link #writable} before this method gets called.
   *
   * @param bytes serialized Writable data.
   * @param writable Writable instance into which data should be stored.
   * @return {@code writable}.
   * @throws IllegalArgumentException if either argument is {@code null}.
   * @throws IOException
   */
  protected <T extends Writable> T readFields(byte[] bytes, T writable) throws IOException {
    Preconditions.checkNotNull(bytes, "Input bytes are null");
    Preconditions.checkNotNull(writable, "Writable is null");
    buf.reset(bytes, bytes.length);
    writable.readFields(buf);
    return writable;
  }

  @Override
  public String bytesToCharArray(byte[] bytes) throws IOException {
    return toCharArray(writable = readFields(bytes, writable));
  }

  @Override
  public Integer bytesToInteger(byte[] bytes) throws IOException {
    return toInteger(writable = readFields(bytes, writable));
  }

  @Override
  public Long bytesToLong(byte[] bytes) throws IOException {
    return toLong(writable = readFields(bytes, writable));
  }

  @Override
  public Float bytesToFloat(byte[] bytes) throws IOException {
    return toFloat(writable = readFields(bytes, writable));
  }

  @Override
  public Double bytesToDouble(byte[] bytes) throws IOException {
    return toDouble(writable = readFields(bytes, writable));
  }

  @Override
  public Map<String, Object> bytesToMap(byte[] bytes) throws IOException {
    return toMap(writable = readFields(bytes, writable));
  }

  @Override
  public Tuple bytesToTuple(byte[] bytes, ResourceFieldSchema schema) throws IOException {
    return toTuple(writable = readFields(bytes, writable), schema);
  }

  @Override
  public DataBag bytesToBag(byte[] bytes, ResourceFieldSchema schema) throws IOException {
    return toBag(writable = readFields(bytes, writable), schema);
  }

  protected String toCharArray(W writable) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Integer toInteger(W writable) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Long toLong(W writable) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Float toFloat(W writable) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Double toDouble(W writable) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Map<String, Object> toMap(W writable) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Tuple toTuple(W writable, ResourceFieldSchema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected DataBag toBag(W writable, ResourceFieldSchema schema) throws IOException {
    throw new UnsupportedOperationException();
  }
}
