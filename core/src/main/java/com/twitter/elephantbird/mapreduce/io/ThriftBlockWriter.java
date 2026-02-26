package com.twitter.elephantbird.mapreduce.io;

import java.io.OutputStream;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.util.TypeRef;

/**
 * A class to write blocks of Thrift data of type M.
 * See {@link ProtobufBlockWriter} for more documentation.
 */
public class ThriftBlockWriter<M extends TBase<?, ?>> extends BinaryBlockWriter<M> {

  public ThriftBlockWriter(OutputStream out, Class<M> protoClass) {
    super(out, protoClass, new ThriftConverter<M>(new TypeRef<M>(protoClass){}), DEFAULT_NUM_RECORDS_PER_BLOCK);
  }

  public ThriftBlockWriter(OutputStream out, Class<M> protoClass, int numRecordsPerBlock) {
    super(out, protoClass, new ThriftConverter<M>(new TypeRef<M>(protoClass){}), numRecordsPerBlock);
  }
}
