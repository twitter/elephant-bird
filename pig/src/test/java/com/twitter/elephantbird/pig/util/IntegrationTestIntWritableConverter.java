package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.io.IntWritable;

/**
 * @author Andy Schlaikjer
 */
public class IntegrationTestIntWritableConverter extends
    AbstractTestWritableConverter<IntWritable, IntWritableConverter> {
  private static final IntWritable[] DATA = { new IntWritable(1), new IntWritable(2),
          new IntWritable(3) };
  private static final String[] EXPECTED = { "1", "2", "3" };

  public IntegrationTestIntWritableConverter() {
    super(IntWritable.class, IntWritableConverter.class, "", DATA, EXPECTED, "int");
  }
}
