package com.twitter.elephantbird.mapreduce.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.protobuf.ByteString;

public interface BlockReader<M> {

  public void close() throws IOException;

  public void setInputStream(InputStream in);

  /**
   * Return byte blob for the next proto object. null indicates end of stream;
   */
  public ByteString readNextProtoByteString() throws IOException;

  public void markNoMoreNewBlocks();

  /**
   * Finds next block marker and reads the block. If skipIfStartingOnBoundary is set
   * skips the the first block if the marker starts exactly at the current position.
   * (i.e. there were no bytes from previous block before the start of the marker).
   */
  public List<ByteString> parseNextBlock(boolean skipIfStartingOnBoundary) throws IOException;
}
