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
 * If a filename ends with ".lzo" and there is no codec found, it is
 * treated as an error (most often the cause is missing LZO libraries).<p>
 *
 * Non-LZO files are also read. If there is a codec configurated, the
 * input is opened with the codec, otherwise it is opened as a normal file.
 *
 * <b>Error handling:</b><br>
 * A small fraction of bad records are tolerated. When deserialization
 * of a record results in an exception or a null object, an a warning
 * is logged. If the rate of errors crosses a threshold
 * (default is 0.0001 or 0.01%) a RuntimeException is thrown.
 * The threshold can be set with configuration variable
 * <code>elephantbird.mapred.input.bad.record.threshold</code>.
 * A value of 0 disables error handling. <p>
 */
public abstract class LzoRecordReader<K, V> extends RecordReader<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoRecordReader.class);

  public static final String BAD_RECORD_THRESHOLD_CONF_KEY = "elephantbird.mapred.input.bad.record.threshold";
  protected long start_;
  protected long pos_;
  protected long end_;
  private FSDataInputStream fileIn_;

  protected InputErrorTracker errorTracker;

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

    errorTracker = new InputErrorTracker(job);

    LOG.info("input split: " + file + " " + start_ + ":" + end_);

    FileSystem fs = file.getFileSystem(job);
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec == null) {
      // throw error only for lzo files, could be a run time lzo library error
      if (file.getName().endsWith(".lzo")) {
        throw new IOException("No codec for file " + file + " found, cannot run");
      }
      // else open it as a simple file
    }

    // Open the file and seek to the start of the split.
    fileIn_ = fs.open(split.getPath());
    InputStream inStream = (codec == null) ?
                           fileIn_ : codec.createInputStream(fileIn_);

    // Creates input stream and also reads the file header.
    createInputReader(inStream, job);

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

  /**
   * Tracks number of of errors in input and throws a Runtime exception
   * if the rate of errors crosses a limit.<p>
   *
   * The intention is to skip over very rare file corruption or incorrect
   * input, but catch programmer errors (incorrect format, or incorrect
   * deserializers etc).
   */
  static class InputErrorTracker {
    long numRecords;
    long numErrors;

    double errorThreshold; // max fraction of errors allowed

    InputErrorTracker(Configuration conf) {
      //default threshold : 0.01%
      errorThreshold = conf.getFloat(BAD_RECORD_THRESHOLD_CONF_KEY, 0.0001f);
      numRecords = 0;
      numErrors = 0;
    }

    void incRecords() {
      numRecords++;
    }

    void incErrors(Throwable cause) {
      numErrors++;
      if (numErrors > numRecords) {
        // incorrect use of this class
        throw new RuntimeException("Forgot to invoke incRecords()?");
      }

      if (cause == null) {
        cause = new Exception("Unknown error");
      }

      if (errorThreshold <= 0) { // no errors are tolerated
        throw new RuntimeException("error while reading input records", cause);
      }

      LOG.warn("Error while reading an input record ("
          + numErrors + " out of " + numRecords + " so far ): ", cause);

      double errRate = numErrors/(double)numRecords;

      // will always excuse the first error. We can decide if single
      // error crosses threshold inside close() if we want to.
      if (numErrors > 1 && errRate > errorThreshold) {
        LOG.error(numErrors + " out of " + numRecords
            + " crosses configured threshold (" + errorThreshold + ")");
        throw new RuntimeException("error rate while reading input records crossed threshold", cause);
      }
    }
  }
}
