package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockReader;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 */
public class LzoThriftBlockRecordReader<M extends TBase<?, ?>> extends LzoBinaryBlockRecordReader<M, ThriftWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftBlockRecordReader.class);

  public LzoThriftBlockRecordReader(TypeRef<M> typeRef) {
    // input stream for the reader will be set by LzoBinaryBlockRecordReader
    super(typeRef, new ThriftBlockReader<M>(null, typeRef), new ThriftWritable<M>(typeRef));
    LOG.info("LzoThriftBlockRecordReader, type args are " + typeRef.getRawClass());
  }
}

