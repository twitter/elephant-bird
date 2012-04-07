package com.twitter.elephantbird.pig.load;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.UnitTestUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

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

    PigServer pigServer = UnitTestUtil.makePigServer();
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
  
  @Test
  public void testNestedLoad() throws IOException {
    
    File tempFile = File.createTempFile("json", null);
    tempFile.deleteOnExit();

    FileWriter writer = new FileWriter(tempFile);
    // json structure as in Twitter Streaming
    writer.write(
        "{" +
        "  \"entities\": {" +
        "    \"hashtags\": [" +
    		"      {\"indices\": [0,0], \"text\": \"test1\"}," +
    		"      {\"indices\": [0,0], \"text\": \"test2\"}" +
    		"    ]," +
    		"    \"user_mentions\": []," +
    		"    \"urls\": []" +
    		"  }" +
    		"}");
    writer.close();

    // extract hashtags from it
    PigServer pigServer = UnitTestUtil.makePigServer();
    // enable nested load
    pigServer.getPigContext().getProperties().setProperty(JsonLoader.NESTED_ENABLED_KEY, "true");
    logAndRegisterQuery(pigServer, "data = load '" + tempFile.getAbsolutePath()
        + "' using com.twitter.elephantbird.pig.load.JsonLoader() as (json: map[]);");
    logAndRegisterQuery(pigServer, "a = foreach data generate json#'entities'#'hashtags' as h;");
    logAndRegisterQuery(pigServer, "b = foreach a generate flatten(h) as h;");
    logAndRegisterQuery(pigServer, "c = foreach b generate h#'text' as h;");
    Iterator<Tuple> tuples = pigServer.openIterator("c");

    int count = 0;
    String[] hashtags = {"test1","test2"};
    while(tuples.hasNext()) {
      Tuple t = tuples.next();
      Assert.assertEquals(hashtags[count], t.get(0).toString());
      count++;
    }
    
    Assert.assertEquals(2, count); // expect two tuples
  }
  
  @Test
  public void testBackwardsCompatibility() throws IOException {
    
    File tempFile = File.createTempFile("json", null);
    tempFile.deleteOnExit();

    FileWriter writer = new FileWriter(tempFile);
    String json = "{\"a\":{\"b\":{\"c\":0}, \"d\":{\"e\":0}}}";
    writer.write(json);
    writer.close();

    // extract hashtags from it
    PigServer pigServer = UnitTestUtil.makePigServer();
    logAndRegisterQuery(pigServer, "data = load '" + tempFile.getAbsolutePath()
        + "' using com.twitter.elephantbird.pig.load.JsonLoader() as (json: map[]);");
    logAndRegisterQuery(pigServer, "a = foreach data generate json#'a' as h;");
    Iterator<Tuple> tuples = pigServer.openIterator("a");

    int count = 0;
    while(tuples.hasNext()) {
      Tuple t = tuples.next();
      Assert.assertEquals(String.class, t.get(0).getClass());
      count++;
    }
    
    Assert.assertEquals(1, count); // expect one tuple
  }
  
  @Test
  public void tesFieldsSpec() throws IOException {
    
    String json = "{\"a\":{\"b\":{\"c\":0}, \"d\":{\"e\":0}}}";
    JsonLoader jsonLoader = new JsonLoader(TextInputFormat.class.getName(),"-fieldsSpec=a,b,c -nestedLoadEnabled");
    Tuple result = jsonLoader.parseStringToTuple(json);
    Map<String, Object> m = (Map<String, Object>)result.get(0);
    Assert.assertTrue(m.containsKey("a"));
    m = (Map<String, Object>)m.get("a");
    Assert.assertTrue(m.containsKey("b"));
    Assert.assertTrue(!m.containsKey("d"));
  }

  private void logAndRegisterQuery(PigServer pigServer, String query) throws IOException {
    LOG.info("Registering query: " + query);
    pigServer.registerQuery(query);
  }
}
