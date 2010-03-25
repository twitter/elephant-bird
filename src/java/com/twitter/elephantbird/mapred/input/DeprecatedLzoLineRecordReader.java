package com.twitter.elephantbird.mapred.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

/**
 * A line-based record reader for LZO files that outputs <position, line>
 * writable pairs in the style of TextInputFormat.
 */

@SuppressWarnings("deprecation")
public class DeprecatedLzoLineRecordReader implements RecordReader<LongWritable, Text> {
  private CompressionCodecFactory codecFactory_ = null;
  private long start_;
  private long pos_;
  private final long end_;
  private final LineReader in_;
  private final FSDataInputStream fileIn_;

  DeprecatedLzoLineRecordReader(Configuration conf, FileSplit split) throws IOException {
    start_ = split.getStart();
    end_ = start_ + split.getLength();
    Path file = split.getPath();

    FileSystem fs = file.getFileSystem(conf);
    codecFactory_ = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory_.getCodec(file);
    if (codec == null) {
      throw new IOException("No LZO codec found, cannot run.");
    }

    // Open the file and seek to the next split.
    fileIn_ = fs.open(file);
    // Create input stream and read the file header.
    in_ = new LineReader(codec.createInputStream(fileIn_), conf);
    if (start_ != 0) {
      fileIn_.seek(start_);

      // Read and ignore the first line.
      in_.readLine(new Text());
      start_ = fileIn_.getPos();
    }

    pos_ = start_;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public boolean next(LongWritable key, Text value) throws IOException {
    // Since the LZOP codec reads everything in LZO blocks, we can't stop if pos == end.
    // Instead, wait for the next block to be read in when pos will be > end.
    while (pos_ <= end_) {
      key.set(pos_);

      int newSize = in_.readLine(value);
      if (newSize == 0) {
        return false;
      }
      pos_ = fileIn_.getPos();
      return true;
    }
    return false;
  }

  public float getProgress() throws IOException {
    if (start_ == end_) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos_ - start_)/ (float)(end_ - start_));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos_;
  }

  public synchronized void close() throws IOException {
    if (in_ != null) {
      in_.close();
    }
  }
}
