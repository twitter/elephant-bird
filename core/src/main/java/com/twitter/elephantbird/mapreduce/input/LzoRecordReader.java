package com.twitter.elephantbird.mapreduce.input;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.twitter.elephantbird.util.HadoopCompat;
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
 * An abstract base class that handles setting up all LZO-based RecordReaders. <p>
 *
 * <b>Error handling:</b><br>
 * A small fraction of bad records are tolerated. When deserialization
 * of a record results in an exception or a null object, an a warning
 * is logged. If the rate of errors crosses a threshold
 * (default is 0.0001 or 0.01%) a RuntimeException is thrown. <br>
 * By default, the threshold is checked at the <i>end</i> of reading.
 * The threshold can be set with configuration variable
 * <code>elephantbird.mapred.input.bad.record.threshold</code>.
 * A value of 0 disables error handling. <p>
 */
public abstract class LzoRecordReader<K, V> extends RecordReader<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoRecordReader.class);

  public static final String BAD_RECORD_THRESHOLD_CONF_KEY = "elephantbird.mapred.input.bad.record.threshold";
  /* Error out only after threshold rate is reached and we have see this many errors */
  public static final String BAD_RECORD_MIN_COUNT_CONF_KEY = "elephantbird.mapred.input.bad.record.min";
  public static final String BAD_RECORD_CHECK_AT_CLOSE = // long name is ok, not usually explicitly set.
      "elephantbird.mapred.input.bad.record.check.only.in.close";

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
    Configuration job = HadoopCompat.getConfiguration(context);

    errorTracker = new InputErrorTracker(job);

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

  @Override
  public void close() throws IOException {
    if (errorTracker != null) {
      errorTracker.close();
    }
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
  static class InputErrorTracker implements Closeable {
    long numRecords;
    long numErrors;

    final double errorThreshold; // max fraction of errors allowed
    final long minErrors; // throw error only after this many errors

    final boolean checkOnlyInClose;

    InputErrorTracker(Configuration conf) {
      //default threshold : 0.01%
      errorThreshold = conf.getFloat(BAD_RECORD_THRESHOLD_CONF_KEY, 0.0001f);
      minErrors = conf.getLong(BAD_RECORD_MIN_COUNT_CONF_KEY, 2);
      checkOnlyInClose = conf.getBoolean(BAD_RECORD_CHECK_AT_CLOSE, true);
      numRecords = 0;
      numErrors = 0;
    }

    void incRecords() {
      numRecords++;
    }

    @Override
    public void close() throws IOException {
      // should throw RTE or an IOE?
      // some applications might ignore IOE from close().
      try {
        if (numErrors > 0) {
          checkErrorThreshold(null);
        }
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    void incErrors(Throwable cause) {
      numErrors++;
      if (numErrors > numRecords) {
        // incorrect use of this class
        throw new RuntimeException("Forgot to invoke incRecords()?");
      }
      if (!checkOnlyInClose) {
        checkErrorThreshold(cause);
      }
    }

    private void checkErrorThreshold(Throwable cause) {
      if (numErrors > 0 && errorThreshold <= 0) { // no errors are tolerated
        throw new RuntimeException("error while reading input records", cause);
      }

      LOG.warn("Error while reading an input record ("
          + numErrors + " out of " + numRecords + " so far ): ", cause);

      double errRate = numErrors/(double)numRecords;

      // will always excuse the first error. We can decide if single
      // error crosses threshold inside close() if we want to.
      if (numErrors >= minErrors  && errRate > errorThreshold) {
        LOG.error(numErrors + " out of " + numRecords
            + " crosses configured threshold (" + errorThreshold + ")");
        throw new RuntimeException("error rate while reading input records crossed threshold", cause);
      }
    }
  }
}
