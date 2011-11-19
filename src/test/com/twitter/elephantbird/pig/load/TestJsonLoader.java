package com.twitter.elephantbird.pig.load;

import junit.framework.TestCase;
import org.apache.pig.data.Tuple;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

/**
 * Test the JsonLoader, make sure it reads the data properly.
 */

public class TestJsonLoader extends TestCase {

  /**
   * {@link JsonLoader#parseStringToTuple(String)} must not choke on lines
   * containing the word "null" (i.e. not the null value but the string
   * "null").
   *
   * This can happen when the original input line to JSONParser contains "null"
   * as a string.  In this case {@link JSONParser#parse(java.io.Reader)} will
   * return a null reference.
   *
   */
  @Test
  public void testNullString() {
      String nullString = "null";
      JsonLoader jsonLoader = new JsonLoader();
      Tuple result = jsonLoader.parseStringToTuple(nullString);
      assertEquals("Parsing line with contents 'null'", null, result);
  }

}
