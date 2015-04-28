package com.twitter.elephantbird.mapreduce.io;

import java.io.OutputStream;

import com.twitter.elephantbird.util.TypeRef;

public class GenericBlockWriter<M> extends BinaryBlockWriter<M> {

  public GenericBlockWriter(OutputStream out, BinaryConverter<M> conv, Class<M> recordClass) {
    super(out, recordClass, conv, DEFAULT_NUM_RECORDS_PER_BLOCK);
  }

  public GenericBlockWriter(OutputStream out, BinaryConverter<M> conv, Class<M> recordClass, int numRecordsPerBlock) {
    super(out, recordClass, conv, numRecordsPerBlock);
  }
}
