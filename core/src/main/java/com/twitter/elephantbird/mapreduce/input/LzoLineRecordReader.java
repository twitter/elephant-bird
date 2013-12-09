package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;


/**
 * Reads line from an lzo compressed text file. Treats keys as offset in file
 * and value as line. <p>
 *
 * Set <code>{@value #MAX_LINE_LEN_CONF}</code> in order to limit size of
 * a line read into memory. Otherwise, some very long lines (100s of MB long)
 * could cause OOM errors and other timeouts.
 */
public class LzoLineRecordReader extends LzoRecordReader<LongWritable, Text> 
    implements MapredInputFormatCompatible<LongWritable, Text> {

  /**
   * Sets maximum number of bytes to read into memory when the input line is very long.
   * The line read is truncated to this size.
   */
  public static final String MAX_LINE_LEN_CONF = "elephantbird.line.recordreader.max.line.length";

  private LineReader in_;

  private LongWritable key_ = new LongWritable();
  private Text value_ = new Text();
  private int maxLineLen = Integer.MAX_VALUE;

  @Override
  public synchronized void close() throws IOException {
    super.close();
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
    maxLineLen = conf.getInt(MAX_LINE_LEN_CONF, Integer.MAX_VALUE);
    in_ = new LineReader(input, conf);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      in_.readLine(new Text(), maxLineLen);
    }
  }

  @Override
  public void setKeyValue(LongWritable key, Text value) {
    key_ = key;
    value_ = value;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    if (pos_ <= end_) {
      key_.set(pos_);

      int newSize = in_.readLine(value_, maxLineLen);
      if (newSize == 0) {
        return false;
      }
      pos_ = getLzoFilePos();

      return true;
    }

    return false;
  }
}

