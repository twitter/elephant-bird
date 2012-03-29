package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TestMiniMRWithDFS;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Test;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;

/**
 * Use mapreduce.TextInputFormat with old mapred interface
 * using {@link DeprecatedInputFormatWrapper}
 */
@SuppressWarnings("deprecation")
public class TestDeprecatedInputFormatWrapper {

  private static boolean usingWrapper = false;

  @Test
  public void testWithTextInputFormat() throws IOException {
    MiniMRCluster mr = null;

    try {
      mr = new MiniMRCluster(4, "file:///", 1);

      JobConf jobConf = new JobConf(mr.createJobConf()) {
        @Override
        public void setInputFormat(Class<? extends InputFormat> mapredFormat) {
          if (mapredFormat == DeprecatedInputFormatWrapper.class) {
            return;
          }
          Assert.assertEquals(org.apache.hadoop.mapred.TextInputFormat.class, mapredFormat);

          DeprecatedInputFormatWrapper.setInputFormat(TextInputFormat.class, this);
          usingWrapper = true;
        }
      };

      TestMiniMRWithDFS.runWordCount(mr, jobConf);
      Assert.assertTrue(usingWrapper);
    } finally {
      if (mr != null) {
        //mr.shutdown();
      }
    }
  }
}
