package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.io.IntWritable;

/**
 * @author Andy Schlaikjer
 */
public class TestIntWritableConverter extends
    AbstractTestWritableConverter<IntWritable, IntWritableConverter> {
  private static final IntWritable V1 = new IntWritable(1);
  private static final IntWritable V2 = new IntWritable(2);
  private static final IntWritable V3 = new IntWritable(3);
  private static final IntWritable[] DATA = { V1, V2, V3 };
  private static final String[] EXPECTED = { "1", "2", "3" };

  public TestIntWritableConverter() {
    super(IntWritableConverter.class, null, IntWritable.class, DATA, EXPECTED, "int");
  }
}
