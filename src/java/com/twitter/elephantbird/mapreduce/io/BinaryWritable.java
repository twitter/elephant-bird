
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

  protected M message;
  protected byte[] messageBytes; // deserialization is delayed till get() or deserialize()
  private BinaryConverter<M> converter;
  protected boolean deserialized = false, serialized = false;

  protected BinaryWritable(M message, BinaryConverter<M> converter) {
    this.message = message;
    this.converter = converter;
  }

  /** throws an exception if the converter is not set */
  private void checkConverter() {
    if (converter == null) {
      throw new IllegalStateException("Runtime parameterized Protobuf/Thrift class is unkonwn. " +
                                      "This object was probably created with default constructor. " +
                                      "Please use setConverter(Class).");
    }
  }

  protected abstract BinaryConverter<M> getConverterFor(Class<M> clazz);

  /**
   * Sets the handler for serialization and deserialization based on the class.
   * This converter is often set in constructor. But some times it might be
   * impossible to know the the actual class during construction. <br> <br>
   *
   * E.g. when this writable is used as output value for a Mapper,
   * MR creates writable on the Reducer using the default constructor,
   * and there is no way for us to know the parameterized class.
   * In this case, user invokes setConverter() before
   * calling get() to supply parameterized class. <br>
   *
   * The class name could be written as part of writable serialization, but we
   * don't yet see a need to do that as it has many other disadvantages.
   */
  public void setConverter(Class<M> clazz) {
    converter = getConverterFor(clazz);
  }

  /**
   * Returns the current object. <br>
   * The deserialization of the actual Protobuf/Thrift object is delayed till
   * the first call to this method. <br>
   * In some cases the the parameterized proto class may not be known yet
   * ( in case of default construction. see {@link #setConverter(Class)} ),
   * and this will throw an {@link IllegalStateException}.
   */
  public M get() {
    if (!deserialized && messageBytes != null) {
      checkConverter();
      message = converter.fromBytes(messageBytes);
      deserialized = true;
    }
    return message;
  }

  public void clear() {
    message = null;
    messageBytes = null;
    deserialized = false;
    serialized = false;
  }

  public void set(M message) {
    this.message = message;
    messageBytes = null;
    serialized = false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = serialize();

    if (bytes != null) {
      out.writeInt(bytes.length);
      out.write(bytes, 0, bytes.length);
    } else {
      out.writeInt(0);
    }
  }

  /**
   * Converts the message to raw bytes, and caches the converted value.
   * @return converted value, which may be null in case of null message or error.
   */
  protected byte[] serialize() {
    if (message != null && serialized == false ) {
      checkConverter();
      messageBytes = converter.toBytes(message);
      serialized = true;
      if (messageBytes == null) {
        // should we throw an IOException instead?
        LOG.warn("Could not serialize " + message.getClass());
      }
    }
    return messageBytes;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (size > 0) {
      messageBytes = new byte[size];
      in.readFully(messageBytes, 0, size);
      // messageBytes is deserialized in get()
      deserialized = false;
    }
  }

  @Override
  public int compareTo(BinaryWritable<M> other) {
    byte[] thisBytes = serialize();
    byte[] otherBytes = other.serialize();
    return BytesWritable.Comparator.compareBytes(thisBytes, 0, thisBytes.length,
        otherBytes, 0, otherBytes.length);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    BinaryWritable<?> other;
    try {
      other = (BinaryWritable<?>)obj;
    } catch (ClassCastException e) {
      return false;
    }

    // Force deserialization of both objects
    get();
    other.get();

    if (message != null) {
      return message.equals(other.message);
    }
    if (other.message == null) {
      return converter.equals(other.converter);
    }

    return false;
  }

  @Override
  public int hashCode() {
    get();
    return 31 + ((message == null) ? 0 : message.hashCode());
  }

  @Override
  public String toString() {
    get();
    if (message == null) {
      return super.toString();
    }
    return message.toString();
  }

}
