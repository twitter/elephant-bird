package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadoop.compression.lzo.LzoCodec;

/**
 * Common test utilities
 */
public class UnitTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(UnitTestUtil.class);


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

  static public String getTestDataDir(Class<?> testClass) {
    return System.getProperty("test.build.data") + "/" + testClass.getName();
  }

  static public boolean isNativeLzoLoaded(Configuration conf) {
    boolean lzoOk = false;
    try {
      lzoOk = LzoCodec.isNativeLzoLoaded(conf);
    } catch (UnsatisfiedLinkError e) {
      LOG.warn("Unable to load native LZO, skipping tests that require it.", e);
    }
    return lzoOk;
  }
}
