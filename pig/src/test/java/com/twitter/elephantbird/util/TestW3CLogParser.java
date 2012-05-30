package com.twitter.elephantbird.util;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.elephantbird.util.W3CLogParser;

import static org.junit.Assert.assertEquals;

public class TestW3CLogParser {
  static final String FIELD_FILE = "W3CLogParser.field.txt";
  static final String VALID_FILE  = "W3CLogParser.valid.txt";
  static final String INVALID_FILE  = "W3CLogParser.invalid.txt";

  private InputStream is_ = null;
  private W3CLogParser parser_ = null;

  @Before
  public void setUp() throws Exception {
    is_ =  getClass().getClassLoader().getResourceAsStream(FIELD_FILE);
    parser_ = new W3CLogParser(is_);
  }

  @After
  public void tearDown() throws Exception {
    is_.close();
  }

  @Test
  public final void testValid() throws IOException, ExecException, Exception {
    verify(VALID_FILE, true);
  }

  @Test
  public final void testInvalid() throws IOException, ExecException, Exception {
    verify(INVALID_FILE, false);
  }

  private void verify(String fileName, boolean expected) throws IOException, ExecException, Exception {
    InputStream inputs = getClass().getClassLoader().getResourceAsStream(fileName);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputs, "UTF-8"));
    String line = null;
    Map<String, String> map = null;
    Boolean results = true;

    try {
      while ((line = reader.readLine()) != null) {
        try {
          map = parser_.parse(line);
          System.out.print(map.get("hostname"));
        } catch(Exception e) {
          results = false;
        }
        assertEquals(results, expected);
      }
    } finally {
      inputs.close();
    }
  }

}
