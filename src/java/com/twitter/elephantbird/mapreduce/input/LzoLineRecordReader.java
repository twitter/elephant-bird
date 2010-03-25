package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * Reads line from an lzo compressed text file. Treats keys as offset in file
 * and value as line.
 */
public class LzoLineRecordReader extends LzoRecordReader<LongWritable, Text> {

  private LineReader in_;

  private final LongWritable key_ = new LongWritable();
  private final Text value_ = new Text();

  @Override
  public synchronized void close() throws IOException {
    if (in_ != null) {
      in_.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value_;
  }

  @Override
  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    in_ = new LineReader(input, conf);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      in_.readLine(new Text());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    while (pos_ <= end_) {
      key_.set(pos_);

      int newSize = in_.readLine(value_);
      if (newSize == 0) {
        return false;
      }
      pos_ = getLzoFilePos();

      return true;
    }

    return false;
  }
}

