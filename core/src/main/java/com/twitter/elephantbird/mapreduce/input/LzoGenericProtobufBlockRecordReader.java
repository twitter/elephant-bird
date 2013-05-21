package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Similar to the LzoProtobufBlockReader classes, but instead of returning serialized
 * objects as values, returns <position, bytes> pairs.  The bytes can be deserialized
 * into objects by the user if desired.
 */
public class LzoGenericProtobufBlockRecordReader extends LzoRecordReader<LongWritable, BytesWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoGenericProtobufBlockRecordReader.class);

  private final LongWritable key_;
  private final BytesWritable value_;

  private ProtobufBlockReader<Message> reader_;

  public LzoGenericProtobufBlockRecordReader() {
    LOG.info("LzoProtobufRecordReader constructor");
    key_ = new LongWritable();
    value_ = new BytesWritable();
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (reader_ != null) {
      reader_.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value_;
  }

  @Override
  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    reader_ = new ProtobufBlockReader<Message>(input, new TypeRef<Message>(){});
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // No need to skip to the sync point here; the block reader will do it for us.
    LOG.debug("LzoProtobufRecordReader.skipToNextSyncPoint called with atFirstRecord = " + atFirstRecord);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // If we are past the end of the file split, tell the reader not to read any more new blocks.
    // Then continue reading until the last of the reader's already-parsed values are used up.
    // The next split will start at the next sync point and no records will be missed.
    if (pos_ > end_) {
      reader_.markNoMoreNewBlocks();
    }
    while (reader_.readProtobufBytes(value_)) {
      key_.set(pos_);
      pos_ = getLzoFilePos();
      return true;
    }

    return false;
  }
}

