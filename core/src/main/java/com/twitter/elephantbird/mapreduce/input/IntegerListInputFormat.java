package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

public class IntegerListInputFormat extends InputFormat<LongWritable, NullWritable> {
  private static final Log LOG = LogFactory.getLog(IntegerListInputFormat.class);

  protected static long min = 1;
  protected static long max = 1;
  protected static long numSplits = 1;

  public static void setListInterval(long max) {
    setListInterval(1, max);
  }

  public static void setListInterval(long min, long max) {
    IntegerListInputFormat.min = min;
    IntegerListInputFormat.max = max;
  }

  public static void setNumSplits(long numSplits) {
    IntegerListInputFormat.numSplits = numSplits;
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    List<InputSplit> splits = Lists.newArrayList();
    // Divide and round up.
    long valuesPerSplit = 1 + (max - min) / numSplits;
    LOG.info("IntegerListInputFormat creating about " + numSplits + " splits for range [" + min + ", " +
              max + "], about " + valuesPerSplit + " values per split");

    for (int i = 0; i < numSplits; ++i) {
      long start = i * valuesPerSplit + min;
      long stop = Math.min((i + 1) * valuesPerSplit + min - 1, max);
      if (start <= stop) {
        splits.add(new IntegerListInputSplit(start, stop));
      }
    }
    LOG.info("IntegerListInputFormat actually created " + splits.size() + " input splits.");
    return splits;
  }

  @Override
  public RecordReader<LongWritable, NullWritable> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new IntegerListRecordReader();
  }
}
