package com.twitter.elephantbird.mapreduce.input;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.util.TypeRef;

public class LzoGenericB64LineRecordReader<M> extends LzoBinaryB64LineRecordReader<M, GenericWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoGenericB64LineRecordReader.class);

  public LzoGenericB64LineRecordReader(TypeRef<M> typeRef, BinaryConverter<M> converter) {
    super(typeRef, new GenericWritable<M>(converter), converter);
    LOG.info("record type is " + typeRef.getRawClass());
  }
}

