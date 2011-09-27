package com.twitter.elephantbird.pig.util;

import org.apache.hadoop.io.Text;

/**
 * @author Andy Schlaikjer
 */
public class IntegrationTestTextConverter extends
    AbstractTestWritableConverter<Text, TextConverter> {
  private static final String V1 = "one, two, buckle my shoe";
  private static final String V2 = "three, four, knock on my door";
  private static final String V3 = "five, six, pickup sticks";
  private static final Text[] DATA = { new Text(V1), new Text(V2), new Text(V3) };
  private static final String[] EXPECTED = { V1, V2, V3 };

  public IntegrationTestTextConverter() {
    super(Text.class, TextConverter.class, "", DATA, EXPECTED, "chararray");
  }
}
