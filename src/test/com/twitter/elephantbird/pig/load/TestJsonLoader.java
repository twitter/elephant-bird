package com.twitter.elephantbird.pig.load;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 * Test the JsonLoader, make sure it reads the data properly.
 */

public class TestJsonLoader {
  private static final Logger LOG = LoggerFactory.getLogger(TestJsonLoader.class);

  /**
   * {@link JsonLoader#parseStringToTuple(String)} must not choke on lines
   * containing the word "null" (i.e. not the null value but the string
   * "null").
   * <p/>
   * This can happen when the original input line to JSONParser contains "null"
   * as a string.  In this case {@link JSONParser#parse(java.io.Reader)} will
   * return a null reference.
   */
  @Test
  public void testNullString() {
    String nullString = "null";
    JsonLoader jsonLoader = new JsonLoader();
    Tuple result = jsonLoader.parseStringToTuple(nullString);
    Assert.assertEquals("Parsing line with contents 'null'", null, result);
  }

  @Test
  public void testPigScript() throws IOException {
    File tempFile = File.createTempFile("json", null);
    tempFile.deleteOnExit();

    FileWriter writer = new FileWriter(tempFile);
    writer.write("{\"score\": 10}\n");
    writer.write("{\"score\": 20}\n");
    writer.write("{\"score\": 30}\n");
    writer.close();

    PigServer pigServer = new PigServer(ExecType.LOCAL);
    logAndRegisterQuery(pigServer, "data = load '" + tempFile.getAbsolutePath()
        + "' using com.twitter.elephantbird.pig.load.JsonLoader() as (json: map[]);");
    logAndRegisterQuery(pigServer, "a = foreach data generate (int) json#'score' as score;");
    logAndRegisterQuery(pigServer, "b = group a all;");
    logAndRegisterQuery(pigServer, "c = foreach b generate SUM(a.score) as total_score;");
    Iterator<Tuple> tuples = pigServer.openIterator("c");

    int count = 0;
    while(tuples.hasNext()) {
      Tuple t = tuples.next();
      Assert.assertEquals(new Long(60), t.get(0)); // expected sum of scores
      count++;
    }

    Assert.assertEquals(1, count); // expect just one tuple
  }

  private void logAndRegisterQuery(PigServer pigServer, String query) throws IOException {
    LOG.info("Registering query: " + query);
    pigServer.registerQuery(query);
  }
}
