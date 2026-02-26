package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

/**
 * This class is not strictly necessary, you can use LzoBinaryB64LineRecordWriter directly.<br>
 * It is just there to make the Protobuf dependency clear.
 *
 * @param <M> Message you are writing
 * @param <W> Writable of this message
 */
public class LzoProtobufB64LineRecordWriter<M extends Message, W extends ProtobufWritable<M>>
extends LzoBinaryB64LineRecordWriter<M, W> {

  public LzoProtobufB64LineRecordWriter(BinaryConverter<M> converter, DataOutputStream out) {
    super(converter, out);
  }
}
