package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.io.BooleanWritable;

/**
 * @author Xu Wenhao
 */
public class IntegrationTestBooleanWritableConverter extends
    AbstractTestWritableConverter<BooleanWritable, BooleanWritableConverter> {
  private static final BooleanWritable[] DATA = { new BooleanWritable(true), new BooleanWritable(false)};
  private static final String[] EXPECTED = { "1", "0" };

  public IntegrationTestBooleanWritableConverter() {
    super(BooleanWritable.class, BooleanWritableConverter.class, "", DATA, EXPECTED, "int");
  }
}
