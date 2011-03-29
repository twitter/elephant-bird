package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.util.Base64;
import com.twitter.elephantbird.util.Protobufs;
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
  private PigToThrift<T> pigToThrift;
  private ThriftConverter<T> converter;

  public LzoThriftB64LinePigStorage(String thriftClassName) {
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    pigToThrift = PigToThrift.newInstance(typeRef);
    converter = ThriftConverter.newInstance(typeRef);
  }

  @Override
public void putNext(Tuple f) throws IOException {
    if (f == null) return;
    T tObj = pigToThrift.getThriftObject(f);
    os_.write(Base64.encodeBytesToBytes(converter.toBytes(tObj)));
    os_.write(Protobufs.NEWLINE_UTF8_BYTE);
  }
}
