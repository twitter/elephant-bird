package com.twitter.elephantbird.mapreduce.input;

import junit.framework.TestCase;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import com.twitter.elephantbird.pig.load.JsonLoader;

/**
 * Test the LzoTextInputFormat, make sure it splits the file properly and
 * returns the right data.
 */
public class TestLzoJsonRecordReader extends TestCase {

  /**
   * {@link LzoJsonRecordReader#decodeLineToJson(JSONParser, Text, MapWritable)}
   * must not choke on lines containing the word "null" (i.e. not the null
   * value but the string "null").
   *
   * This can happen when the original input line to JSONParser contains "null"
   * as a string.  In this case {@link JSONParser#parse(java.io.Reader)} will
   * return a null (!) value (not a string of value "null").
   *
   */
  @Test
  public void testNullString() {
      Text line = new Text("null");
      boolean result = LzoJsonRecordReader.decodeLineToJson(new JSONParser(), line, new MapWritable());
      assertEquals("Parsing string with value 'null'", false, result);
  }

}
