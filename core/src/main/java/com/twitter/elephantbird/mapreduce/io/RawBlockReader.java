package com.twitter.elephantbird.mapreduce.io;

import java.io.InputStream;

/**
 * A {@link BinaryBlockReader} that returns each record as uninterpreted
 * raw bytes.
 */
public class RawBlockReader extends BinaryBlockReader<byte[]> {

  public RawBlockReader(InputStream in) {
    super(in, new IdentityBinaryConverter(), false);
  }
}
