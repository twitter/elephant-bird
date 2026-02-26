package com.twitter.elephantbird.pig.util;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * Common test utilities
 */
public class PigTestUtil {

  /**
   * Creates a new PigServer in local mode.
   * Sets pig properties for lzo codec and temp directory.
   */
  static public PigServer makePigServer() throws ExecException {

    PigServer pigServer = new PigServer(ExecType.LOCAL);
    // set lzo codec:
    pigServer.getPigContext().getProperties().setProperty(
        "io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");

    pigServer.getPigContext().getProperties().setProperty(
        "pig.temp.dir", System.getProperty("test.build.data") + "/pig-temp");

    return pigServer;
  }
}
