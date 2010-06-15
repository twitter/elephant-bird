package com.twitter.elephantbird.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.twitter.elephantbird.util.TypeRef;

/**
 * A Hadoop Writable wrapper around a thrift object of type M.
 */
public class ThriftB64LineWritable<T extends TBase> implements WritableComparable<ThriftB64LineWritable<T>> {
  private final TSerializer serializer_;
  private final TDeserializer deserializer_;
  private final Base64 base64_;

  private final TypeRef<T> typeRef_;
  private T thriftObj_ = null;
  
  public ThriftB64LineWritable(TypeRef<T> typeRef) {
    this(null, typeRef);
  }

  public ThriftB64LineWritable(T thriftObj, TypeRef<T> typeRef) {
    typeRef_ = typeRef;
    thriftObj_ = thriftObj;

    serializer_ = new TSerializer();
    deserializer_ = new TDeserializer();
    base64_ = new Base64();
  }

  public T get() {
    return thriftObj_;
  }
  
  public int getLength() {
    if (thriftObj_ != null) {
      try {
        return base64_.encode(serializer_.serialize(thriftObj_)).length;
      } catch (TException e) {
      }
    }
    return 0;
  }
  
  public void clear() {
    thriftObj_ = null;
  }

  public void set(T thriftObj) {
    thriftObj_ = thriftObj;
  }

  public void readFields(DataInput in) throws IOException {
    if (thriftObj_ == null) {
      thriftObj_ = typeRef_.safeNewInstance();
    }
    String line = in.readLine();
    byte[] byteArray = base64_.decode(line.getBytes("UTF-8"));
    try {
      deserializer_.deserialize(thriftObj_, byteArray);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public void write(DataOutput out) throws IOException {
    try {
      byte[] byteArray = base64_.encode(serializer_.serialize(thriftObj_));
      out.write(byteArray);
      out.write("\n".getBytes("UTF-8"));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

	@Override
  public int compareTo(ThriftB64LineWritable<T> other) {
    try {
    	byte[] thriftObjBytes = serializer_.serialize(thriftObj_);
	    byte[] otherBytes = serializer_.serialize(other.get());
	    return BytesWritable.Comparator.compareBytes(thriftObjBytes, 0, thriftObjBytes.length, otherBytes, 0, otherBytes.length);
    } catch (TException e) {
    	throw new RuntimeException(e);
    }
  }
}


