package com.twitter.elephantbird.pig.load;

import junit.framework.TestCase;
import org.apache.pig.data.Tuple;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

/**
 * Test the LzoTextInputFormat, make sure it splits the file properly and
 * returns the right data.
 */
public class TestJsonLoader extends TestCase {

  /**
   * {@link JsonLoader#parseStringToTuple(String)} must not choke on lines
   * containing the word "null" (i.e. not the null value but the string
   * "null").
   *
   * This can happen when the original input line to JSONParser contains "null"
   * as a string.  In this case {@link JSONParser#parse(java.io.Reader)} will
   * return a null (!) value (not a string of value "null").
   *
   */
  @Test
  public void testNullString() {
      String nullString = "null";
      JsonLoader jsonLoader = new JsonLoader();
      Tuple result = jsonLoader.parseStringToTuple(nullString);
      assertEquals("Parsing string with value 'null'", null, result);
  }

}
