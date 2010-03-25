package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.io.InputStream;

import com.hadoop.compression.lzo.LzopInputStream;
import org.apache.pig.impl.io.BufferedPositionedInputStream;

/**
 * A wrapper around Pig's BufferedPositionedInputStream that counts the number of
 * compressed bytes read rather than the number of decompressed bytes read.  Necessary
 * because the filesizes and splits are based off of compressed sizes, but the BufferedPositionedInputStream
 * only knows about uncompressed bytes read.  This screws up input splits.
 * Note that this class behaves identically to BufferedPositionedInputStream if the stream given in the
 * constructor is NOT an LzopInputStream.
 */
public class LzoBufferedPositionedInputStream extends BufferedPositionedInputStream {
  private LzopInputStream lzopIn_ = null;
  private long start_ = 0;

  /**
   * Store the input stream and start position if it's an LzopInputStream.
   * @param in the input stream
   * @param pos the starting position in the file.
   */
  public LzoBufferedPositionedInputStream(InputStream in, long pos) {
    super(in, pos);

    if (in instanceof LzopInputStream) {
      lzopIn_ = (LzopInputStream)in;
      start_ = pos;
    }
  }

  /**
   * If it's an LzopInputStream, return the compressed bytes read plus the offset.  Otherwise,
   * default to superclass behavior.
   */
  @Override
  public long getPosition() throws IOException {
    if (lzopIn_ != null) {
      return start_ + lzopIn_.getCompressedBytesRead();
    } else {
      return super.getPosition();
    }
  }

  /**
   * super.skip() skips uncompressed bytes, so we need to override it.
   * This allows us to specify skip size in a compressed stream.
   */
  @Override
  public long skip(long toSkip) throws IOException {
    long pos = getPosition();
    while (getPosition() - pos < toSkip) {
      if (super.skip(toSkip) == 0) break;
    }
    return getPosition() - pos;
  }

}
