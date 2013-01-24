package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IntegerListRecordReader extends RecordReader<LongWritable, NullWritable> {
  private static final Log LOG = LogFactory.getLog(IntegerListRecordReader.class);

  // Avoid recreating the object each time getCurrentKey is called.
  protected LongWritable key = new LongWritable();
  // Save the split for later use.
  protected IntegerListInputSplit split;
  // The current position in the split's range.
  protected long cur;

  public IntegerListRecordReader() {}

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // Save the split for later.
    split = (IntegerListInputSplit)inputSplit;
    // Initialize cur_ to one less than the given min so that it can be
    // consistently incremented in nextKeyValue
    cur = split.getMin() - 1;

    LOG.info("Creating IntegerListRecordReader with InputSplit [" + split.getMin() + ", " + split.getMax() + "]");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    ++cur;
    key.set(cur);
    return cur <= split.getMax();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (cur - split.getMin() + 1) / (float)split.getLength();
  }

  @Override
  public void close() throws IOException {}
}
