package com.twitter.elephantbird.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IntegerListInputSplit extends InputSplit implements Writable {
  private static final Log LOG = LogFactory.getLog(IntegerListInputSplit.class);

  protected long min;
  protected long max;

  public IntegerListInputSplit() {}

  public IntegerListInputSplit(long min, long max) {
    if (min > max) {
      throw new IllegalArgumentException("Attempt to create IntegerListInputSplit with min > max, min = " +
          min + " and max = " + max);
    }
    LOG.info("Creating IntegerListInputSplit with InputSplit [" + min + ", " + max + "]");
    this.min = min;
    this.max = max;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return max - min + 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[] {};
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(min);
    dataOutput.writeLong(max);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    min = dataInput.readLong();
    max = dataInput.readLong();
  }
}
