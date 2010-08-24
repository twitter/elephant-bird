package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer for LZO-encoded protobuf blocks, generally read by
 * a ProtobufBlockWriter or similar.
 */

public class LzoProtobufBlockRecordWriter<M extends Message, W extends ProtobufWritable<M>>
    extends RecordWriter<NullWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockRecordWriter.class);

  protected final TypeRef typeRef_;
  protected final DataOutputStream out_;
  protected final ProtobufBlockWriter writer_;

  public LzoProtobufBlockRecordWriter(TypeRef<M> typeRef, DataOutputStream out) {
    typeRef_ = typeRef;
    out_ = out;
    writer_ = new ProtobufBlockWriter(out_, typeRef_.getRawClass());
  }

  public void write(NullWritable nullWritable, W protobufWritable)
      throws IOException, InterruptedException {
    writer_.write(protobufWritable.get());
  }

  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    writer_.finish();
    out_.close();
  }
}
