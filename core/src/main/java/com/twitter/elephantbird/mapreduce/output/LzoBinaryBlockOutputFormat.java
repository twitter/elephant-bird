package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.io.RawBlockWriter;
import com.twitter.elephantbird.mapreduce.io.RawBytesWritable;

/**
 * Output format for LZO block-compressed byte[] records.
 *
 * @author Andy Schlaikjer
 * @see LzoBinaryBlockRecordWriter
 * @see RawBytesWritable
 */
public class LzoBinaryBlockOutputFormat extends LzoOutputFormat<byte[], RawBytesWritable> {
  @Override
  public RecordWriter<byte[], RawBytesWritable> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    return new LzoBinaryBlockRecordWriter<byte[], RawBytesWritable>(new RawBlockWriter(
        getOutputStream(job)));
  }
}
