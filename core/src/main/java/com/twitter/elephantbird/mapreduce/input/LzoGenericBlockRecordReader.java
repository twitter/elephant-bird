package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.GenericBlockReader;
import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.util.TypeRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.google.protobuf.ByteString;

import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic lzo block record reader that uses the provided BinaryConverter to deserialize data.
 */
public class LzoGenericBlockRecordReader<M>
    extends LzoBlockRecordReader<M, GenericWritable<M>, GenericBlockReader> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoGenericBlockRecordReader.class);

  private Counter recordsSkippedCounter;

  private final BinaryConverter<M> deserializer_;

  public LzoGenericBlockRecordReader(TypeRef<M> typeRef, BinaryConverter<M> binaryConverter) {
    super(typeRef, new GenericBlockReader(null), new GenericWritable<M>(binaryConverter));
    deserializer_ = binaryConverter;
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                                     throws IOException, InterruptedException {
    String group = "LzoBlocks of " + typeRef_.getRawClass().getName();
    recordsSkippedCounter = HadoopUtils.getCounter(context, group, "Records Skipped");

    super.initialize(genericSplit, context);
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
