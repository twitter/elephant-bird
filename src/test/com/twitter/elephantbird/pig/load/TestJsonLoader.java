package com.twitter.elephantbird.pig.load;

import junit.framework.TestCase;
import org.apache.pig.data.Tuple;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import java.util.Map;

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

  @Test
  public void testMultiLevel() throws Exception {
      String multiLvlJsonString = "{\"a\":{\"b\":\"c\"}}";
      JsonLoader jsonLoader = new JsonLoader();
      Tuple result = jsonLoader.parseStringToTuple(multiLvlJsonString);
      assertEquals("Expecting 1 element in tuple", 1, result.getAll().size());
      Object element = result.get(0);
      assertTrue("Expecting element to be an Map", element instanceof Map);
      assertEquals("Expecting key to be 'a'", "a", ((Map) element).keySet().iterator().next());
      Map innerMap = (Map)((Map) element).get("a");
      assertTrue("Expecting first elemant of inner map should be 'b'", innerMap.containsKey("b"));
      assertEquals("Expecting to get 'c' value for key 'a' in inerMap", "c", innerMap.get("b"));
  }

}
