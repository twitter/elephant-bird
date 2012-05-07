package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A RecordReader equivalent of FilterInputStream
 */
public class FilterRecordReader<K, V> extends RecordReader<K, V> {

  protected RecordReader<K, V> reader;

  public FilterRecordReader(RecordReader<K, V> reader) {
    this.reader = reader;

  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return reader.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return reader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return reader.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    reader.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return reader.nextKeyValue();
  }
}
