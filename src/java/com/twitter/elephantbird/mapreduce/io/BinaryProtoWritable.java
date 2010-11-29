
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
public abstract class BinaryProtoWritable<M> implements WritableComparable<BinaryProtoWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryProtoWritable.class);

  private M message;
  private BinaryProtoConverter<M> protoConverter;
  
  protected BinaryProtoWritable(M message, BinaryProtoConverter<M> protoConverter) {
    this.message = message;
    this.protoConverter = protoConverter;
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
      bytes = protoConverter.toBytes(message);
      if (bytes == null) {
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
      message = protoConverter.fromBytes(messageBytes);
    }
  }

  @Override
  public int compareTo(BinaryProtoWritable<M> other) {
    byte[] bytes = protoConverter.toBytes(message);
    byte[] otherBytes = protoConverter.toBytes(other.get());
    return BytesWritable.Comparator.compareBytes(bytes, 0, bytes.length, otherBytes, 0, otherBytes.length);
  }
}
