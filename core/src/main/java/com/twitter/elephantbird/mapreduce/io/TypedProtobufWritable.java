package com.twitter.elephantbird.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.protobuf.Message;
import org.apache.hadoop.io.WritableUtils;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M with support for
 * polymorphism. This class writes the type of M to the output which makes it
 * unnecessary to pass in the correct Class<M> when creating the object and also
 * allows reading/writing heterogeneous data:
 * <code>
 * for (TypedProtobufWritable<Message> value in values) {
 *     Message message = value.get();
 *     if (message instanceof MyProtocMessage1) {
 *         MyProtocMessage1 protoc1 = (MyProtocMessage1) message;
 *         // do something with the protoc1
 *     }
 * }
 * </code>
 *
 * Since this class writes the type of M to the output, it is recommended to use
 * some kind of compression on the output.
 */
public class TypedProtobufWritable<M extends Message> extends ProtobufWritable<M> {
  public TypedProtobufWritable() {
    super(null, null);
  }

  public TypedProtobufWritable(M obj) {
    super(obj, null);
    if (obj != null) {
      this.setConverter((Class) obj.getClass());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    String className = WritableUtils.readString(in);
    if (!className.isEmpty()) {
      Class clazz;
      try {
        clazz = Class.forName(className);
      } catch (ClassNotFoundException ex) {
        throw new IllegalArgumentException("Byte stream does not have a "
          + "valid class identifier", ex);
      }
      this.setConverter(clazz);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    M obj = this.get();
    if (obj == null) {
      WritableUtils.writeString(out, "");
    } else {
      WritableUtils.writeString(out, obj.getClass().getName());
    }
  }

  @Override
  public void set(M m) {
    super.set(m);
    if (m != null) {
      this.setConverter((Class) m.getClass());
    }
  }
}
