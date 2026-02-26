package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An inputformat for LZO-compressed JSON files.  Returns <position, json object>
 * pairs, where the json object in java is essentially a Map<String, Object>.
 *
 * WARNING: The RecordReader-derived class used here may not handle multi-line json
 * well, if it all.  Please improve this.
 */
public class LzoJsonInputFormat extends LzoInputFormat<LongWritable, MapWritable> {

  @Override
  public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {

    return new LzoJsonRecordReader();
  }
}
