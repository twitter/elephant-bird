package com.twitter.elephantbird.cascading2.scheme;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;

/**
 * Scheme for Thrift/Base64 encoded log files.
 *
 * @author Avi Bryant, Ning Liang
 */
public class LzoThriftB64LineScheme extends LzoB64LineScheme {

  private transient BinaryConverter<TBase> converter;
  private Class thriftClass;

  public LzoThriftB64LineScheme(Class thriftClass) {
    this.thriftClass = thriftClass;
  }

  protected BinaryConverter<TBase> getConverter() {
    if (converter == null) {
      converter = ThriftConverter.newInstance(thriftClass);
    }
    return converter;
  }

  protected Object decodeMessage(byte[] bytes) {
    return getConverter().fromBytes(bytes);
  }

  protected byte[] encodeMessage(Object message) {
    return getConverter().toBytes((TBase) message);
  }
}
