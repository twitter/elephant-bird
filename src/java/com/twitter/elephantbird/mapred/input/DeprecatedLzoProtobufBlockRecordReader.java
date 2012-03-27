package com.twitter.elephantbird.mapred.input;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 */

@SuppressWarnings("deprecation")
public class DeprecatedLzoProtobufBlockRecordReader<M extends Message>
    extends DeprecatedLzoBlockRecordReader<M> {

  public DeprecatedLzoProtobufBlockRecordReader(TypeRef<M> typeRef, BinaryWritable<M> writable, Configuration conf, FileSplit split) throws IOException {
    super(typeRef, writable, conf, split);
  }

  protected BinaryBlockReader<M> createInputReader(InputStream input, Configuration conf) throws IOException {
    return new ProtobufBlockReader<M>(input, typeRef_);
  }
}
