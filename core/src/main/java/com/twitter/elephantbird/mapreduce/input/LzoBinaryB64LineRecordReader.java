package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

/**
 * Reads line from an lzo compressed text file, base64 decodes it, and then
 * deserializes that into the templatized object.
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */
public class  LzoBinaryB64LineRecordReader<M, W extends BinaryWritable<M>> extends LzoRecordReader<LongWritable, W> {

  private LineReader lineReader_;

  private final Text line_ = new Text();
  private final LongWritable key_ = new LongWritable();
  private final W value_;
  private TypeRef<M> typeRef_;

  private final Base64 base64_ = Codecs.createStandardBase64();
  private final BinaryConverter<M> converter_;

  private Counter linesReadCounter;
  private Counter emptyLinesCounter;
  private Counter recordsReadCounter;
  private Counter recordErrorsCounter;

  protected LzoBinaryB64LineRecordReader(TypeRef<M> typeRef, W protobufWritable, BinaryConverter<M> protoConverter) {
    typeRef_ = typeRef;
    converter_ = protoConverter;
    value_ = protobufWritable;
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (lineReader_ != null) {
      lineReader_.close();
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
    lineReader_ = new LineReader(input, conf);
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                                      throws IOException, InterruptedException {
    String group = "LzoB64Lines of " + typeRef_.getRawClass().getName();
    linesReadCounter = HadoopUtils.getCounter(context, group, "Lines Read");
    recordsReadCounter = HadoopUtils.getCounter(context, group, "Records Read");
    recordErrorsCounter = HadoopUtils.getCounter(context, group, "Errors");
    emptyLinesCounter = HadoopUtils.getCounter(context, group, "Empty Lines");
    super.initialize(genericSplit, context);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      lineReader_.readLine(new Text());
    }
  }

  /**
   * Read the next key, value pair.
   * <p>
   * A small fraction of bad records in input are tolerated.
   * See  {@link LzoRecordReader} for more information on error handling.
   *
   * @return true if a key/value pair was read
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    while (pos_ <= end_) {
      key_.set(pos_);

      int newSize = lineReader_.readLine(line_);
      if (newSize == 0) {
        return false;
      }
      HadoopCompat.incrementCounter(linesReadCounter, 1);
      pos_ = getLzoFilePos();
      if (line_.getLength() == 0 || line_.charAt(0) == '\n') {
        HadoopCompat.incrementCounter(emptyLinesCounter, 1);
        continue;
      }

      M protoValue = null;

      errorTracker.incRecords();
      Throwable decodeException = null;

      try {
        byte[] lineBytes = Arrays.copyOf(line_.getBytes(), line_.getLength());
        protoValue = converter_.fromBytes(base64_.decode(lineBytes));
      } catch(Throwable t) {
        decodeException = t;
      }

      if (protoValue == null) {
        HadoopCompat.incrementCounter(recordErrorsCounter, 1);
        errorTracker.incErrors(decodeException);
        continue;
      }

      HadoopCompat.incrementCounter(recordsReadCounter, 1);
      value_.set(protoValue);
      return true;
    }

    return false;
  }
}
