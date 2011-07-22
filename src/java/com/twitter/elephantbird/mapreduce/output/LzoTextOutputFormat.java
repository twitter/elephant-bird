package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LzoTextOutputFormat extends LzoOutputFormat<NullWritable, Text> {

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext job)
                                           throws IOException, InterruptedException {
    final DataOutputStream out = getOutputStream(job);

    return new RecordWriter<NullWritable, Text>() {

      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
      }

      public void write(NullWritable key, Text value) throws IOException, InterruptedException {
        out.write(value.getBytes(), 0, value.getLength());
        out.write('\n');
      }
    };
  }
}
