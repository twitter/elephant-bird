package com.twitter.elephantbird.util;

import com.hadoop.compression.lzo.LzoCodec;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Common test utilities
 */
public class CoreTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CoreTestUtil.class);

  static public String getTestDataDir(Class<?> testClass) {
    return System.getProperty("test.build.data") + "/" + testClass.getSimpleName();
  }

  /**
   * @return true if "require.lzo.tests" system property is set or if native
   * lzo libraries are loaded.
   */
  static public boolean okToRunLzoTests(Configuration conf) throws IOException {

    if (Boolean.parseBoolean(System.getProperty("require.lzo.tests"))) {
      return true;
    }
    try {
      return LzoCodec.isNativeLzoLoaded(conf);
    } catch (UnsatisfiedLinkError e) {
      LOG.warn("Unable to load native LZO, skipping tests that require it.", e);
    }
    return false;
  }
}
