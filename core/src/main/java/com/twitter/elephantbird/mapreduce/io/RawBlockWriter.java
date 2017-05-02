package com.twitter.elephantbird.mapreduce.io;

import java.io.OutputStream;

/**
 * A {@link BinaryBlockWriter} where each record is a byte array.
 */
public class RawBlockWriter extends BinaryBlockWriter<byte[]> {

  public RawBlockWriter(OutputStream out) {
    super(out, byte[].class,
        new IdentityBinaryConverter(), DEFAULT_NUM_RECORDS_PER_BLOCK);
  }
}
