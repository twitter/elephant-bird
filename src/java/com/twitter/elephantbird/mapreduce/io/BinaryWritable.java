
package com.twitter.elephantbird.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Hadoop Writable wrapper around a serialized messages like Protocol buffers.
 */
public abstract class BinaryWritable<M> implements WritableComparable<BinaryWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryWritable.class);

  private M message;
  private BinaryConverter<M> converter;

  protected BinaryWritable(M message, BinaryConverter<M> converter) {
    this.message = message;
    this.converter = converter;
  }

  public M get() {
    return message;
  }

  public void clear() {
    message = null;
  }

  public void set(M message) {
    this.message = message;
  }

  public void write(DataOutput out) throws IOException {
    byte[] bytes = null;
    if (message != null) {
      bytes = converter.toBytes(message);
      if (bytes == null) {
        // should we throw an IOException instead?
        LOG.warn("Could not serialize " + message.getClass());
      }
    }
    if (bytes != null) {
      out.writeInt(bytes.length);
      out.write(bytes, 0, bytes.length);
    } else {
      out.writeInt(0);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (size > 0) {
      byte[] messageBytes = new byte[size];
      in.readFully(messageBytes, 0, size);
      message = converter.fromBytes(messageBytes);
    }
  }

  @Override
  public int compareTo(BinaryWritable<M> other) {
    byte[] bytes = converter.toBytes(message);
    byte[] otherBytes = converter.toBytes(other.get());
    return BytesWritable.Comparator.compareBytes(bytes, 0, bytes.length, otherBytes, 0, otherBytes.length);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;

    BinaryWritable<?> other;
    try {
      other = (BinaryWritable<?>)obj;
    } catch (ClassCastException e) {
      return false;
    }
    if (message != null)
      return message.equals(other.message);
    if (other.message == null) // contained objects in both writables are null.
      return converter.equals(other.converter);

    return false;
  }
}
