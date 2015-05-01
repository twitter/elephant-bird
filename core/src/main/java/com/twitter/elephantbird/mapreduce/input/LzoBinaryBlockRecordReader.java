package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.DecodeException;
import com.twitter.elephantbird.util.HadoopCompat;
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
 * a ProtobufBlockWriter or similar.
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */
public class LzoBinaryBlockRecordReader<M, W extends BinaryWritable<M>>
    extends LzoRecordReader<LongWritable, W> implements MapredInputFormatCompatible<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBinaryBlockRecordReader.class);

  private LongWritable key_;
  private W value_;
  private final TypeRef<M> typeRef_;
  boolean updatePosition = false;
  /* make LzoBinaryBlockRecordReader return lzoblock offset the same way as
   * LzoBinaryBlockRecordReader for indexing purposes.
   * For the the first record returned, pos_ should be 0
   * if the recordreader is reading the first split,
   * otherwise it should be end of the current lzo block.
   * This makes pos_ consistent with LzoBinaryB64LineRecordReader.
   */

  private final BinaryBlockReader<M> reader_;

  private Counter recordsReadCounter;
  private Counter recordErrorsCounter;
  private Counter recordsSkippedCounter;

  private final BinaryConverter<M> deserializer_;

  public LzoBinaryBlockRecordReader(TypeRef<M> typeRef, BinaryBlockReader<M> reader, W binaryWritable) {
    key_ = new LongWritable();
    value_ = binaryWritable;
    reader_ = reader;
    typeRef_ = typeRef;
    deserializer_ = reader.getConverter();
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
    recordsSkippedCounter = HadoopUtils.getCounter(context, group, "Records Skipped");
    recordErrorsCounter = HadoopUtils.getCounter(context, group, "Errors");

    super.initialize(genericSplit, context);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // No need to skip to the sync point here; the block reader will do it for us.
    LOG.debug("LzoProtobufBlockRecordReader.skipToNextSyncPoint called with atFirstRecord = " + atFirstRecord);
    updatePosition = !atFirstRecord;
    // except for the first split, skip a protobuf block if it starts exactly at the split boundary
    // because such a block would be read by the previous split (note comment about 'pos_ > end_'
    // in nextKeyValue() below)
    reader_.parseNextBlock(!atFirstRecord);
  }

  @Override
  public void setKeyValue(LongWritable key, W value) {
    key_ = key;
    value_ = value;
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
    // If we are past the end of the file split, tell the reader not to read any more new blocks.
    // Then continue reading until the last of the reader's already-parsed values are used up.
    while (true) { // loop to skip over bad records
      if (pos_ > end_) {
        reader_.markNoMoreNewBlocks();
        // Why not pos_ >= end_, stop when we just reach the end?
        // we don't know if we have read all the bytes uncompressed in the current lzo block,
        // only way to make sure that we have read all of the split is to read till the
        // first record that has at least one byte in the next split.
        // As a consequence of this, next split reader skips at least one byte
        // (i.e. skips either partial or full record at the beginning).
      }
      byte[] byteArray = reader_.readNextProtoBytes();

      if(byteArray == null) {
        return false;
      }

      errorTracker.incRecords();
      M decoded = null;
      try {
        decoded = deserializer_.fromBytes(byteArray);
      } catch (DecodeException e) {
        errorTracker.incErrors(e);
        HadoopCompat.incrementCounter(recordErrorsCounter, 1);
        continue;
      }

      if (updatePosition) {
        pos_ = getLzoFilePos();
        updatePosition = false;
      }

      if (decoded != null) {
        key_.set(pos_);
        value_.set(decoded);
        pos_ = getLzoFilePos();

        HadoopCompat.incrementCounter(recordsReadCounter, 1);
        return true;
      } else {
        HadoopCompat.incrementCounter(recordsSkippedCounter, 1);
      }
    }
  }
}
