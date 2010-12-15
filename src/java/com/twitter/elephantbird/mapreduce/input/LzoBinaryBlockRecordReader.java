package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 */

public class LzoBinaryBlockRecordReader<M, W extends BinaryWritable<M>> extends LzoRecordReader<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBinaryBlockRecordReader.class);

  private final LongWritable key_;
  private final W value_;
  private final TypeRef<M> typeRef_;

  private final BinaryBlockReader<M> reader_;

  private Counter recordsReadCounter;
  private Counter recordErrorsCounter;

  public LzoBinaryBlockRecordReader(TypeRef<M> typeRef, BinaryBlockReader<M> reader, W binaryWritable) {
    key_ = new LongWritable();
    value_ = binaryWritable;
    reader_ = reader;
    typeRef_ = typeRef;
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
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                                     throws IOException, InterruptedException {
    String group = "LzoBlocks of " + typeRef_.getRawClass().getName();
    recordsReadCounter = HadoopUtils.getCounter(context, group, "Records Read");
    recordErrorsCounter = HadoopUtils.getCounter(context, group, "Errors");

    super.initialize(genericSplit, context);
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
      if (value_.get() == null) {
        recordErrorsCounter.increment(1);
      }
      recordsReadCounter.increment(1);
      key_.set(pos_);
      pos_ = getLzoFilePos();
      return true;
    }

    return false;
  }
}

