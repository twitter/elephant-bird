package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.Base64;
import com.twitter.elephantbird.util.Protobufs;

/**
 * A RecordWriter-derived class for use with the LzoProtobufB64LineOutputFormat.
 * Writes data as base64 encoded serialized protocol buffers, one per line.
 */

public class LzoBinaryB64LineRecordWriter<M, W extends BinaryWritable<M>>
    extends RecordWriter<NullWritable, W> {

  private final BinaryConverter<M> protoConverter_;
  private final DataOutputStream out_;

  public LzoBinaryB64LineRecordWriter(BinaryConverter<M> converter, DataOutputStream out) {
    protoConverter_ = converter;
    out_ = out;
  }

  @Override
public void write(NullWritable nullWritable, W protobufWritable)
      throws IOException, InterruptedException {
    byte[] b64Bytes = Base64.encodeBytesToBytes(
            protoConverter_.toBytes(protobufWritable.get()));
    out_.write(b64Bytes);
    out_.write(Protobufs.NEWLINE_UTF8_BYTE);
  }

  @Override
public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    out_.close();
  }
}
