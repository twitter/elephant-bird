package com.twitter.elephantbird.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M.
 *
 * TODO: Implement WritableComparable so it can be used as a key.  Could just
 * use BytesWritable's comparator against the raw protobuf bytes to avoid the need
 * to even serialize objects during comparisons.
 */

public class ProtobufWritable<M extends Message> implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWritable.class);

  private M message_;
  private final Function<byte[], M> protoConverter_;
  
  public ProtobufWritable(TypeRef<M> typeRef) {
    this(null, typeRef);
  }

  public ProtobufWritable(M message, TypeRef<M> typeRef) {
    message_ = message;
    protoConverter_ = Protobufs.getProtoConverter(typeRef.getRawClass());
    LOG.debug("ProtobufWritable, typeClass is " + typeRef.getRawClass() + " and message is " + message_);
  }

  public M get() {
    return message_;
  }

  public int getLength() {
    return message_ != null ? message_.getSerializedSize() : 0;
  }

  public void clear() {
    message_ = null;
  }

  public void set(M message) {
    message_ = message;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(getLength());
    if (message_ != null) {
      byte[] byteArray = message_.toByteArray();
      out.write(byteArray, 0, byteArray.length);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (size > 0) {
      byte[] messageBytes = new byte[size];
      in.readFully(messageBytes, 0, size);
      message_ = protoConverter_.apply(messageBytes);
    }
  }
}
