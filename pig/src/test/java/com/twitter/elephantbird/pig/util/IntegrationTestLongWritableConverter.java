package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.io.LongWritable;

/**
 * @author Andy Schlaikjer
 */
public class IntegrationTestLongWritableConverter extends
    AbstractTestWritableConverter<LongWritable, LongWritableConverter> {
  private static final LongWritable[] DATA = { new LongWritable(1), new LongWritable(2),
          new LongWritable(4294967296l) };
  private static final String[] EXPECTED = { "1", "2", "4294967296" };

  public IntegrationTestLongWritableConverter() {
    super(LongWritable.class, LongWritableConverter.class, "", DATA, EXPECTED, "long");
  }
}
