package com.twitter.elephantbird.mapred.output;

import com.google.protobuf.Message;

import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

/**
 * A writer for LZO-encoded blocks of Protobuf objects.
 *
 */
public class DeprecatedLzoProtobufBlockRecordWriter<M extends Message>
    extends DeprecatedLzoBinaryBlockRecordWriter<M, ProtobufWritable<M>> {
  public DeprecatedLzoProtobufBlockRecordWriter(ProtobufBlockWriter<M> writer) {
    super(writer);
  }
}
