
package com.twitter.elephantbird.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Hadoop Writable wrapper around a serialized messages like Protocol buffers.
 */
public abstract class BinaryWritable<M> implements WritableComparable<BinaryWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryWritable.class);

  // NOTE: only one of message and messageBytes is non-null at any time so that
  // message and messageBytes don't go out of sync (user could modify message).
  private M message;
  private byte[] messageBytes;
  private BinaryConverter<M> converter;

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
   * sets converter. useful for reusing existing converter.
   */
  public void setConverter(BinaryConverter<M> converter) {
    this.converter = converter;
  }

  /**
   * Returns the current object. Subsequent calls to get() may not return the
   * same object, but in stead might return a new object deserialized from same
   * set of bytes. As a result, multiple calls to get() should be avoided, and
   * modifications to an object returned by get() may not
   * reflect even if this writable is serialized later. <br>
   * Please use set() to be certain of what object is serialized.<br><br>
   *
   * The deserialization of the actual Protobuf/Thrift object is often delayed
   * till the first call to this method. <br>
   * In some cases the the parameterized proto class may not be known yet
   * ( in case of default construction. see {@link #setConverter(Class)} ),
   * and this will throw an {@link IllegalStateException}.
   */
  public M get() {
    // may be we should rename this method. the contract would be less
    // confusing with a different name.
    if (message == null && messageBytes != null) {
      checkConverter();
      return converter.fromBytes(messageBytes);
    }
    return message;
  }

  public void clear() {
    message = null;
    messageBytes = null;
  }

  public void set(M message) {
    this.message = message;
    this.messageBytes = null;
    // should we serialize the object to messageBytes instead?
    // that is the only way we can guarantee any subsequent modifications to
    // message by the user don't affect serialization. Unlike Protobuf objects
    // Thrift objects are mutable. For now we will delay deserialization until
    // it is required.
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
  private byte[] serialize() {
    if (messageBytes == null && message != null) {
      checkConverter();
      messageBytes = converter.toBytes(message);
      if (messageBytes == null) {
        // should we throw an IOException instead?
        LOG.warn("Could not serialize " + message.getClass());
      } else {
        message = null; // so that message and messageBytes don't go out of
                        // sync.
      }
    }
    return messageBytes;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    message = null;
    messageBytes = null;
    int size = in.readInt();
    if (size > 0) {
      byte[] buf = new byte[size];
      in.readFully(buf, 0, size);
      messageBytes = buf;
      // messageBytes is deserialized in get()
    }
  }

  @Override
  public int compareTo(BinaryWritable<M> other) {
    byte[] thisBytes = serialize();
    byte[] otherBytes = other.serialize();
    int thisLen = thisBytes == null ? 0 : thisBytes.length;
    int otherLen = otherBytes == null ? 0 : otherBytes.length;
    return BytesWritable.Comparator.compareBytes(thisBytes, 0, thisLen,
        otherBytes, 0, otherLen);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    BinaryWritable<M> other;
    try {
      other = (BinaryWritable<M>)obj;
    } catch (ClassCastException e) {
      return false;
    }

    return compareTo(other) == 0;
  }

  /**
   * <p>Returns a hashCode that is based on the serialized bytes.
   * This makes the hash stable across multiple instances of JVMs.
   * (<code>hashCode()</code> is not required to return the same value in
   * different instances of the same applications in Java, just in a
   * single instance of the application; Hadoop imposes a more strict requirement.)
   * <br>
   * In addition, it may not be feasible to create a deserialized object from
   * the serialized bytes (see {@link #setConverter(Class)})
   */
  @Override
  public int hashCode() {
    byte[] bytes = serialize();
    return (bytes == null) ? 31 : Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    M msgObj = null;
    try {
      msgObj = get();
    } catch (IllegalStateException e) {
      // It is ok. might not be able to avoid this case in some situations.
      return super.toString() + "{could not be deserialized}";
    }
    if (msgObj == null) {
      return super.toString();
    }
    return msgObj.toString();
  }
}
