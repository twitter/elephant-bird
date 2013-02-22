package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.twitter.elephantbird.pig.store.SequenceFileStorage;

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

  @Test
  public void testDefaultCtor() throws IOException {
    pigServer.registerQuery(String.format("A = LOAD 'file:%s' USING %s();", tempFilename,
        SequenceFileStorage.class.getName()));
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void testDefaultCtor02() throws IOException {
    pigServer.registerQuery(String.format("A = LOAD 'file:%s' USING %s('', '');", tempFilename,
        SequenceFileStorage.class.getName()));
    validate(pigServer.openIterator("A"));
  }
}
