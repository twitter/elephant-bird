package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.Protobufs;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;

/**
 * A RecordWriter-derived class for use with the LzoProtobufB64LineOutputFormat.
 * Writes data as base64 encoded serialized protocol buffers, one per line.
 */

public class LzoBinaryB64LineRecordWriter<M, W extends BinaryWritable<M>>
    extends RecordWriter<M, W> {

  private final BinaryConverter<M> converter;
  private final DataOutputStream out;
  private final Base64 base64;

  public LzoBinaryB64LineRecordWriter(BinaryConverter<M> converter, DataOutputStream out) {
    this.converter = converter;
    this.out = out;
    this.base64 = Codecs.createStandardBase64();
  }

  @Override
  public void write(M nullWritable, W writable)
      throws IOException, InterruptedException {
    byte[] b64Bytes = base64.encode(converter.toBytes(writable.get()));
    out.write(b64Bytes);
    out.write(Protobufs.NEWLINE_UTF8_BYTE);
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    out.close();
  }

  // for convenience
  public static <M extends TBase<?, ?>> LzoBinaryB64LineRecordWriter<M, ThriftWritable<M>>
               newThriftWriter(Class<M> tClass, DataOutputStream out) {
    return new LzoBinaryB64LineRecordWriter<M, ThriftWritable<M>>
                            (ThriftConverter.newInstance(tClass), out);
  }

  // for convenience
  public static <M extends Message> LzoBinaryB64LineRecordWriter<M, ProtobufWritable<M>>
               newProtobufWriter(Class<M> tClass, DataOutputStream out) {
    return new LzoBinaryB64LineRecordWriter<M, ProtobufWritable<M>>
                            (ProtobufConverter.newInstance(tClass), out);
  }
}
