package com.twitter.elephantbird.mapreduce.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 */
public class LzoProtobufBlockRecordReader<M extends Message, W extends ProtobufWritable<M>> extends LzoBinaryBlockRecordReader<M, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockRecordReader.class);

  public LzoProtobufBlockRecordReader(TypeRef<M> typeRef, W protobufWritable) {
    // input stream for the reader will be set by LzoBinaryBlockRecordReader
    super(typeRef, new ProtobufBlockReader<M>(null, typeRef), protobufWritable);
    LOG.info("LzoProtobufBlockRecordReader, type args are " + typeRef.getRawClass());
  }
}

