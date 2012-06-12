package com.twitter.elephantbird.mapreduce.output;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class LzoProtobufBlockRecordWriter <M extends Message, W extends ProtobufWritable<M>>
extends LzoBinaryBlockRecordWriter<M, W> {

  public LzoProtobufBlockRecordWriter(BinaryBlockWriter<M> writer) {
    super(writer);
  }

}
