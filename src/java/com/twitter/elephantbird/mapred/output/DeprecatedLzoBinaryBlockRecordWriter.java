package com.twitter.elephantbird.mapred.output;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * A writer for LZO-encoded blocks of protobuf or Thrift objects, generally read by
 * a ProtobufBlockWriter or similar.
 */
public class DeprecatedLzoBinaryBlockRecordWriter<M, W extends BinaryWritable<M>>
    implements RecordWriter<NullWritable, W> {

  private BinaryBlockWriter<M> writer;

  public DeprecatedLzoBinaryBlockRecordWriter(BinaryBlockWriter<M> writer) {
    this.writer = writer;
  }

  public void write(NullWritable nullWritable, W writable)
      throws IOException {
    writer.write(writable.get());
  }

  public void close(Reporter reporter) throws IOException {
    writer.finish();
    writer.close();
  }
}
