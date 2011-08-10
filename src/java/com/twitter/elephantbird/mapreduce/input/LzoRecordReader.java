package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract base class that handles setting up all LZO-based RecordReaders.
 */
public abstract class LzoRecordReader<K, V> extends RecordReader<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoRecordReader.class);

  protected long start_;
  protected long pos_;
  protected long end_;
  private FSDataInputStream fileIn_;

  /**
   * Get the progress within the split.
   */
  @Override
  public float getProgress() {
    if (start_ == end_) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos_ - start_) / (float) (end_ - start_));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos_;
  }

  public long getLzoFilePos() throws IOException {
    return fileIn_.getPos();
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    FileSplit split = (FileSplit) genericSplit;
    start_ = split.getStart();
    end_ = start_ + split.getLength();
    final Path file = split.getPath();
    Configuration job = context.getConfiguration();

    LOG.info("input split: " + file + " " + start_ + ":" + end_);

    FileSystem fs = file.getFileSystem(job);
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec == null) {
      throw new IOException("No codec for file " + file + " found, cannot run");
    }

    // Open the file and seek to the start of the split.
    fileIn_ = fs.open(split.getPath());

    // Creates input stream and also reads the file header.
    createInputReader(codec.createInputStream(fileIn_), job);

    if (start_ != 0) {
      fileIn_.seek(start_);
      skipToNextSyncPoint(false);
      start_ = fileIn_.getPos();
      LOG.info("Start is now " + start_);
    } else {
      skipToNextSyncPoint(true);
    }
    pos_ = start_;
  }

  protected abstract void createInputReader(InputStream input, Configuration conf) throws IOException;
  protected abstract void skipToNextSyncPoint(boolean atFirstRecord) throws IOException;
}
