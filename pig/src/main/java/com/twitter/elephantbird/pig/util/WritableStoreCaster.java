package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

/**
 * Base LoadStoreCaster implementation which simplifies specification of conversion from Pig types
 * to Writable type.
 *
 * @author Andy Schlaikjer
 */
public abstract class WritableStoreCaster<W extends Writable> extends WritableLoadCaster<W>
    implements LoadStoreCaster {
  private final DataOutputBuffer buf = new DataOutputBuffer();

  public WritableStoreCaster(W writable) {
    super(writable);
  }

  public WritableStoreCaster() {
  }

  private byte[] write(Writable writable) throws IOException {
    Preconditions.checkNotNull(writable);
    buf.reset();
    writable.write(buf);
    return buf.getData();
  }

  @Override
  public byte[] toBytes(DataByteArray value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(String value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Boolean value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Integer value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Long value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Float value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Double value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Map<String, Object> value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(Tuple value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(DateTime value) throws IOException {
    return write(toWritable(value));
  }

  @Override
  public byte[] toBytes(DataBag value) throws IOException {
    return write(toWritable(value));
  }

  protected W toWritable(DataByteArray value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(String value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Boolean value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Integer value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Long value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Float value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Double value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Map<String, Object> value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(Tuple value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(DataBag value) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected W toWritable(DateTime value) throws IOException {
    throw new UnsupportedOperationException();
  }
}
