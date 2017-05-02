package com.twitter.elephantbird.mapreduce.input;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.util.TypeRef;

public class  LzoThriftB64LineRecordReader<M extends TBase<?, ?>> extends LzoBinaryB64LineRecordReader<M, ThriftWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftB64LineRecordReader.class);

  public LzoThriftB64LineRecordReader(TypeRef<M> typeRef) {
    super(typeRef, new ThriftWritable<M>(typeRef), new ThriftConverter<M>(typeRef));
    LOG.info("record type is " + typeRef.getRawClass());
  }
}

