package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.mapreduce.io.BinaryProtoWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 */

public class LzoBinaryBlockRecordReader<M, W extends BinaryProtoWritable<M>> extends LzoRecordReader<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBinaryBlockRecordReader.class);

  private final LongWritable key_;
  private final W value_;

  private BinaryBlockReader<M> reader_;

  public LzoBinaryBlockRecordReader(BinaryBlockReader<M> reader, W protobufWritable) {
    key_ = new LongWritable();
    value_ = protobufWritable;
    reader_ = reader;
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader_ != null) {
      reader_.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public W getCurrentValue() throws IOException, InterruptedException {
    return value_;
  }

  @Override
  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    reader_.setInputStream(input);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // No need to skip to the sync point here; the block reader will do it for us.
    LOG.debug("LzoProtobufBlockRecordReader.skipToNextSyncPoint called with atFirstRecord = " + atFirstRecord);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // If we are past the end of the file split, tell the reader not to read any more new blocks.
    // Then continue reading until the last of the reader's already-parsed values are used up.
    // The next split will start at the next sync point and no records will be missed.
    if (pos_ > end_) {
      reader_.markNoMoreNewBlocks();
    }
    if (reader_.readNext(value_)) {
      key_.set(pos_);
      pos_ = getLzoFilePos();
      return true;
    }

    return false;
  }
}

