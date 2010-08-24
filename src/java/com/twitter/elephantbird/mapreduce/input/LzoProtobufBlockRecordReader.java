package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Message;
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

public class LzoProtobufBlockRecordReader<M extends Message, W extends ProtobufWritable<M>>
    extends LzoRecordReader<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockRecordReader.class);

  private final LongWritable key_;
  private final W value_;
  private final TypeRef<M> typeRef_;

  private ProtobufBlockReader<M> reader_;

  public LzoProtobufBlockRecordReader(TypeRef<M> typeRef, W protobufWritable) {
    typeRef_ = typeRef;
    LOG.info("LzoProtobufBlockRecordReader, type args are " + typeRef_.getRawClass());
    key_ = new LongWritable();
    value_ = protobufWritable;
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
    reader_ = new ProtobufBlockReader<M>(input, typeRef_);
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
    while (reader_.readProtobuf(value_)) {
      key_.set(pos_);
      pos_ = getLzoFilePos();
      return true;
    }

    return false;
  }
}

