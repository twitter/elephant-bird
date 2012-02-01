package com.twitter.elephantbird.mapred.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class DeprecatedLzoTextOutputFormat
            extends DeprecatedLzoOutputFormat<NullWritable, Text> {

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {

    final DataOutputStream out = getOutputStream(job);

    return new RecordWriter<NullWritable, Text>() {

      public void close(Reporter reporter) throws IOException {
        out.close();
      }

      public void write(NullWritable key, Text value) throws IOException {
        out.write(value.getBytes(), 0, value.getLength());
        out.write('\n');
      }
    };
  }

}
