package com.twitter.elephantbird.mapred.input;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * A reader for LZO-encoded protobuf blocks, generally written by
 * a ProtobufBlockWriter or similar.  Returns <position, protobuf> pairs.
 *
 * TODO : should be removed. made obsolete by DeprecatedInputFormatWrapper
 */

@SuppressWarnings("deprecation")
public class DeprecatedLzoProtobufBlockRecordReader<M extends Message, W extends ProtobufWritable<M>>
    implements RecordReader<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(DeprecatedLzoProtobufBlockRecordReader.class);
  private final FSDataInputStream fileIn_;

  // Will create a uncompressed input stream from a compressed block.
  private CompressionCodecFactory codecFactory_ = null;

  private final long end_;
  private long start_;
  private long pos_;
  private W value_;

  // Will be assigned by the concrete subclass.
  protected ProtobufBlockReader<M> reader_;
  protected TypeRef<M> typeRef_;

  /**
   * We're doing stuff in the constructor that probably should be handled with an init method - however the
   * hadoop < 0.19 API isn't ideal.
   */
  public DeprecatedLzoProtobufBlockRecordReader(TypeRef<M> typeRef, W protobufWritable, Configuration conf, FileSplit split) throws IOException {
    typeRef_ = typeRef;
    value_ = protobufWritable;
    start_ = split.getStart();
    end_ = start_ + split.getLength();
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(conf);
    codecFactory_ = new CompressionCodecFactory(conf);
    final CompressionCodec codec = codecFactory_.getCodec(file);
    if (codec == null) {
      throw new IOException("No LZO codec found, cannot run.");
    }

    // Open the file and seek to the next split.
    fileIn_ = fs.open(split.getPath());

    createInputReader(codec.createInputStream(fileIn_), conf);

    if (start_ != 0) {
      LOG.debug("Seeking to split start at pos " + start_);
      fileIn_.seek(start_);
      skipToNextSyncPoint(true);
      start_ = fileIn_.getPos();
      LOG.debug("Start is now " + start_);
    } else {
      skipToNextSyncPoint(false);
    }
  }

  @Override
  public boolean next(LongWritable key, W value) throws IOException {
    if (pos_ > end_) {
      reader_.markNoMoreNewBlocks();
    }
    while (reader_.readProtobuf(value)) {
      key.set(pos_);
      pos_ = fileIn_.getPos();
      return true;
    }
    return false;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public W createValue() {
    return value_;
  }

  public float getProgress() throws IOException {
    if (start_ == end_) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos_ - start_) / (float) (end_ - start_));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos_;
  }

  public synchronized void close() throws IOException {
    if (reader_ != null) {
      reader_.close();
    }
  }

  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Nothing to do here because the reader_ does the sync each time it's called on
    // to read a protobuf.
    LOG.debug("DeprecatedLzoProtobufBlockRecordReader.skipToNextSyncPoint called with atFirstRecord = " + atFirstRecord);
  }

  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    reader_ = new ProtobufBlockReader<M>(input, typeRef_);
  }
}
