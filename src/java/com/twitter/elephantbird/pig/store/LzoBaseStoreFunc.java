package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.io.OutputStream;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.pig.StoreFunc;

/**
 * This class serves as the base class for any functions storing output via lzo.
 * It implements the common functions it can, and wraps the given output stream in an lzop
 * output stream.
 */
public abstract class LzoBaseStoreFunc implements StoreFunc {
  protected CompressionOutputStream os_;

  /**
   * Wrap the given output stream in an LzopOutputStream.
   */
  public void bindTo(OutputStream os) throws IOException {
    LzopCodec codec = new LzopCodec();
    codec.setConf(new Configuration());
    os_ = codec.createOutputStream(os);
  }

  /**
   * Clean up resources.
   */
  public void finish() throws IOException {
    if (os_ != null) {
      os_.close();
    }
  }

  /**
   * Nothing special to do here?
   */
  public Class<?> getStorePreparationClass() throws IOException {
    return null;
  }
}
