package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.io.Text;

/**
 * @author Andy Schlaikjer
 */
public class TestTextConverter extends AbstractTestWritableConverter<Text, TextConverter> {
  private static final String V1 = "one, two, buckle my shoe";
  private static final String V2 = "three, four, shut the door";
  private static final String V3 = "five, six, something else";
  private static final Text[] DATA = { new Text(V1), new Text(V2), new Text(V3) };
  private static final String[] EXPECTED = { V1, V2, V3 };

  public TestTextConverter() {
    super(TextConverter.class, null, Text.class, DATA, EXPECTED, "chararray");
  }
}
