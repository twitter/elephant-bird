package com.twitter.elephantbird.mapreduce.input;

import junit.framework.TestCase;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

/**
 * Test the LzoJsonRecordReader, make sure it reads the data properly.
 */
public class TestLzoJsonRecordReader extends TestCase {

  /**
   * {@link LzoJsonRecordReader#decodeLineToJson(JSONParser, Text, MapWritable)}
   * must not choke on lines containing the word "null" (i.e. not the null
   * value but the string "null").
   *
   * This can happen when the original input line to JSONParser contains "null"
   * as a string.  In this case {@link JSONParser#parse(java.io.Reader)} will
   * return a null reference.
   *
   */
  @Test
  public void testNullString() {
      Text line = new Text("null");
      boolean result = LzoJsonRecordReader.decodeLineToJson(new JSONParser(), line, new MapWritable());
      assertEquals("Parsing line with contents 'null'", false, result);
  }

}
