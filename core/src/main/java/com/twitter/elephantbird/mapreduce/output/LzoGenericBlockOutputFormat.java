package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.io.RawBlockWriter;
import com.twitter.elephantbird.mapreduce.io.RawBytesWritable;

public class LzoGenericBlockOutputFormat extends LzoOutputFormat<byte[], GenericWritable> {
  @Override
  public RecordWriter<byte[], RawBytesWritable> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    return new LzoBinaryBlockRecordWriter<byte[], GenericWritable>(new RawBlockWriter(
        getOutputStream(job)));
  }
}
