package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.GenericBlockReader;
import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.util.TypeRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.ByteString;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;
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


public class LzoGenericBlockRecordReader<M>
    extends LzoRecordReader<LongWritable, GenericWritable<M>> implements MapredInputFormatCompatible<LongWritable, GenericWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoGenericBlockRecordReader.class);

  private LongWritable key_;
  private GenericWritable<M> value_;
  private final TypeRef<M> typeRef_;
  boolean updatePosition = false;
  /* make LzoBinaryBlockRecordReader return lzoblock offset the same way as
   * LzoBinaryBlockRecordReader for indexing purposes.
   * For the the first record returned, pos_ should be 0
   * if the recordreader is reading the first split,
   * otherwise it should be end of the current lzo block.
   * This makes pos_ consistent with LzoBinaryB64LineRecordReader.
   */

  private final GenericBlockReader reader_;

  private Counter recordsReadCounter;
  private Counter recordErrorsCounter;
  private Counter recordsSkippedCounter;

  private final BinaryConverter<M> deserializer_;

  public LzoGenericBlockRecordReader(TypeRef<M> typeRef, BinaryConverter<M> binaryConverter) {
    key_ = new LongWritable();
    deserializer_ = binaryConverter;
    reader_ =  new GenericBlockReader(null);
    value_ = new GenericWritable<M>(binaryConverter);
    typeRef_ = typeRef;
    LOG.info("LzoThriftBlockRecordReader, type args are " + typeRef.getRawClass());
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
  public GenericWritable<M> getCurrentValue() throws IOException, InterruptedException {
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
  public void setKeyValue(LongWritable key, GenericWritable<M> value) {
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
      ByteString bs = reader_.readNextProtoByteString();

      if(bs == null) {
        return false;
      }

      errorTracker.incRecords();
      M decoded = null;
      try {
        decoded = deserializer_.fromBytes(bs.toByteArray());
      } catch (Throwable e) {
        errorTracker.incErrors(e);
        HadoopCompat.incrementCounter(recordErrorsCounter, 1);
        continue;
      }

      if (decoded != null) {
        if (updatePosition) {
          pos_ = getLzoFilePos();
          updatePosition = false;
        }
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
