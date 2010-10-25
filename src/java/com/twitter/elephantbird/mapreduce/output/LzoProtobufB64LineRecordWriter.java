package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RecordWriter-derived class for use with the LzoProtobufB64LineOutputFormat.
 * Writes data as base64 encoded serialized protocol buffers, one per line.
 */

public class LzoProtobufB64LineRecordWriter<M extends Message, W extends ProtobufWritable<M>>
    extends RecordWriter<NullWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LineRecordWriter.class);

  protected final TypeRef typeRef_;
  protected final DataOutputStream out_;
  protected final Base64 base64_;

  public LzoProtobufB64LineRecordWriter(TypeRef<M> typeRef, DataOutputStream out) {
    base64_ = new Base64();
    typeRef_ = typeRef;
    out_ = out;
  }

  public void write(NullWritable nullWritable, W protobufWritable)
      throws IOException, InterruptedException {
    byte[] b64Bytes = base64_.encode(protobufWritable.get().toByteArray());
    out_.write(b64Bytes);
    out_.write("\n".getBytes("UTF-8"));
  }

  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    out_.close();
  }
}
