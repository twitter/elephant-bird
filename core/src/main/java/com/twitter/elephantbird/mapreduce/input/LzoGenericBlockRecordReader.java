package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.util.TypeRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.ByteString;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.util.TypeRef;

import org.slf4j.Logger;

/**
 * Generic reader for LZO-encoded protobuf blocks. Uses the supplied BinaryConverter for deserialization.
 */
public class LzoGenericBlockRecordReader<M>
    extends LzoBinaryBlockRecordReader<M, GenericWritable<M>> {

  public LzoGenericBlockRecordReader(TypeRef<M> typeRef, BinaryConverter<M> binaryConverter) {
    super(typeRef,
      new BinaryBlockReader(null, binaryConverter),
      new GenericWritable<M>(binaryConverter));
  }
}
