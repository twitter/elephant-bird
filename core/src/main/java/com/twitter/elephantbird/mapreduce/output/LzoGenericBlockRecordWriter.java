package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LzoGenericBlockRecordWriter<M>
    extends RecordWriter<M, GenericWritable<M>> {

  private BinaryBlockWriter<M> writer_;

  public LzoGenericBlockRecordWriter(BinaryBlockWriter<M> writer) {
    writer_ = writer;
  }

  public void write(M nullWritable, GenericWriteable<M> protoWritable)
      throws IOException, InterruptedException {
    writer_.write(protoWritable.get());
    // the counters are not accessible
  }

  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    writer_.finish();
    writer_.close();
  }
}

