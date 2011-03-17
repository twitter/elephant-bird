package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Serializes Pig Tuples into Base-64 encoded, line-delimited Thrift objects.
 * The fields in the pig tuple must correspond exactly to the fields in
 * the Thrift object, as no name-matching is performed (names of the tuple
 * fields are not currently accessible to a StoreFunc. It will be in 0.7,
 * so something more flexible will be possible)
 */
public class LzoThriftB64LinePigStorage<T extends TBase<?, ?>> extends LzoBaseStoreFunc {

  private TypeRef<T> typeRef;
  private Base64 base64 = new Base64();
  private PigToThrift<T> pigToThrift;
  private ThriftConverter<T> converter;

  public LzoThriftB64LinePigStorage(String thriftClassName) {
    typeRef = ThriftUtils.getTypeRef(thriftClassName);
    pigToThrift = PigToThrift.newInstance(typeRef);
    converter = ThriftConverter.newInstance(typeRef);
  }

  public void putNext(Tuple f) throws IOException {
    if (f == null) return;
    T tObj = pigToThrift.getThriftObject(f);
    os_.write(base64.encode(converter.toBytes(tObj)));
    os_.write(Protobufs.NEWLINE_UTF8_BYTE);
  }
}
