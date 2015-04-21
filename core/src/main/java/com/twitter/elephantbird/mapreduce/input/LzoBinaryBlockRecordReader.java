package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.TypeRef;

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
    extends LzoBlockRecordReader<M, W, BinaryBlockReader<M>> {

  private static final Logger LOG = LoggerFactory.getLogger(LzoBinaryBlockRecordReader.class);

  public LzoBinaryBlockRecordReader(TypeRef<M> typeRef, BinaryBlockReader<M> reader, W binaryWritable) {
    super(typeRef, reader, binaryWritable);
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

      value_.set(null);
      errorTracker.incRecords();
      Throwable decodeException = null;

      try {
        if (!reader_.readNext(value_)) {
          return false; // EOF
        }
      } catch (InvalidProtocolBufferException e) {
        decodeException = e;
      } catch (IOException e) {
        // Re-throw IOExceptions that are not due to protobuf decode errors
        throw e;
      } catch (Throwable e) {
        decodeException = e;
      }

      if (updatePosition) {
        pos_ = getLzoFilePos();
        updatePosition = false;
      }

      key_.set(pos_);
      pos_ = getLzoFilePos();
      if (value_.get() != null) {
        HadoopCompat.incrementCounter(recordsReadCounter, 1);
        return true;
      }
      errorTracker.incErrors(decodeException);
      HadoopCompat.incrementCounter(recordErrorsCounter, 1);
      // continue
    }
  }
}
