package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.twitter.elephantbird.pig.util.PigTestUtil;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestJsonStringToMap {
  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  private final JsonStringToMap udf_ = new JsonStringToMap();

  @Test
  public void testSchema() {
    Schema schema = udf_.outputSchema(null);
    assertNotNull(schema);
    assertEquals("{json: map[chararray]}", schema.toString());
  }

  @Test
  public final void testStandard() throws IOException, ExecException {
    Tuple input = tupleFactory_.newTuple(Arrays.asList("{\"name\": \"value\", \"number\": 2}"));
    Map<String, String> result = udf_.exec(input);
    assertTrue("It should return a Map", result instanceof Map<?, ?>);
    assertEquals("value", result.get("name"));
    assertEquals("It is expected to return numbers as strings", "2", result.get("number"));
  }

  @Test
  public final void testNestedJson() throws IOException, ExecException {
    Tuple input = tupleFactory_.newTuple(Arrays
        .asList("{\"name\": \"value\", \"nestedJson\": {\"json\": \"ihazit\"}}"));
    Map<String, String> result = udf_.exec(input);
    assertTrue("Nested Json should just return as a String",
        result.get("nestedJson") instanceof String);
  }

  @Test
  public final void testInThePig() throws IOException {
    File tempFile = File.createTempFile("test", ".txt");
    String tempFilename = tempFile.getAbsolutePath();
    PrintWriter pw = new PrintWriter(tempFile);
    pw.println("1\t{\"name\": \"bob\", \"number\": 2}");
    pw.close();
    PigServer pig = PigTestUtil.makePigServer();
    try {
      pig.registerQuery(String.format("DEFINE JsonStringToMap %s();",
          JsonStringToMap.class.getName()));
      pig.registerQuery(String
          .format("x = LOAD '%s' AS (id: int, value: chararray);", tempFilename));
      pig.registerQuery(String.format("x = FOREACH x GENERATE id, JsonStringToMap(value);",
          tempFilename));
      Schema schema = pig.dumpSchema("x");
      assertNotNull(schema);
      assertEquals("{id: int,json: map[chararray]}", schema.toString());
      Iterator<Tuple> x = pig.openIterator("x");
      assertNotNull(x);
      assertTrue(x.hasNext());
      Tuple t = x.next();
      assertNotNull(t);
      assertEquals(2, t.size());
      Map<?, ?> actual = (Map<?, ?>) t.get(1);
      assertNotNull(actual);
      Map<String, String> expected = ImmutableMap.<String, String> of("name", "bob", "number", "2");
      assertEquals(expected.size(), actual.size());
      for (Map.Entry<String, String> e : expected.entrySet()) {
        assertEquals(e.getValue(), actual.get(e.getKey()));
      }
    } finally {
      pig.shutdown();
    }
  }
}
