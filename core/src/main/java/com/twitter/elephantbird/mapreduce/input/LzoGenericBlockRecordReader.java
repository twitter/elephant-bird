package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.GenericBlockReader;
import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.util.TypeRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 */
public class LzoGenericBlockRecordReader<M> extends LzoBinaryBlockRecordReader<M, GenericWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoGenericBlockRecordReader.class);

  public LzoGenericBlockRecordReader(TypeRef<M> typeRef, BinaryConverter<M> binaryConverter) {
    // input stream for the reader will be set by LzoBinaryBlockRecordReader
    super(typeRef, new GenericBlockReader<M>(null, typeRef, binaryConverter), new GenericWritable<M>(binaryConverter));
    LOG.info("LzoThriftBlockRecordReader, type args are " + typeRef.getRawClass());
  }
}

