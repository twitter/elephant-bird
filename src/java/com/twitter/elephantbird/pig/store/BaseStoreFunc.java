package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.StoreFunc;

/**
 * Base class for StoreFunc implementations. <br>
 * Implements some of the trivial interface methods.
 */
public abstract class BaseStoreFunc  extends StoreFunc {

  @SuppressWarnings("unchecked")
  protected RecordWriter writer = null;

  @Override
  public void prepareToWrite(@SuppressWarnings("unchecked") RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  /**
   * Invokes writer.write(key, value). <br>
   * The main purpose is to catch {@link InterruptedException} and
   * covert it to an {@link IOException}. Avoids unnecessary try-catch dance
   * in putNext() implementations.
   */
  @SuppressWarnings("unchecked")
  final protected void writeRecord(Object key, Object value) throws IOException {
    try {
      writer.write(key, value);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}

