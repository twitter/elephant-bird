package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.BlockReader;
import com.twitter.elephantbird.util.TypeRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;
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
 * Parent class for lzo based block record readers.
 */
public abstract class LzoBlockRecordReader<M, W, R extends BlockReader>
    extends LzoRecordReader<LongWritable, W> implements MapredInputFormatCompatible<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoRecordReader.class);

  protected LongWritable key_;
  protected W value_;
  protected final TypeRef<M> typeRef_;
  boolean updatePosition = false;
  /* make LzoBlockRecordReader return lzoblock offset the same way as
   * LzoBinaryBlockRecordReader for indexing purposes.
   * For the the first record returned, pos_ should be 0
   * if the recordreader is reading the first split,
   * otherwise it should be end of the current lzo block.
   * This makes pos_ consistent with LzoBinaryB64LineRecordReader.
   */

  public LzoBlockRecordReader(TypeRef<M> typeRef, R reader, W value) {
    typeRef_ = typeRef;
    reader_ = reader;
    key_ = new LongWritable();
    value_ = value;
    LOG.info("LzoBlockRecordReader, type args are " + typeRef.getRawClass());
  }

  protected final R reader_;

  protected Counter recordsReadCounter;
  protected Counter recordErrorsCounter;

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
    recordErrorsCounter = HadoopUtils.getCounter(context, group, "Errors");

    super.initialize(genericSplit, context);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // No need to skip to the sync point here; the block reader will do it for us.
    LOG.debug("LzoBlockRecordReader.skipToNextSyncPoint called with atFirstRecord = " + atFirstRecord);
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
   */
  abstract public boolean nextKeyValue() throws IOException, InterruptedException;
}
