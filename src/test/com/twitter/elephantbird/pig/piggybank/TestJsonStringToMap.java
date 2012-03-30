package com.twitter.elephantbird.pig.piggybank;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

public class TestJsonStringToMap {
  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  JsonStringToMap udf_ = new JsonStringToMap();

  @Test
  public void testSchema() {
    Schema schema = udf_.outputSchema(null);
    assertNotNull(schema);
    assertEquals("{map: map[chararray]}", schema.toString());
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
    Tuple input = tupleFactory_.newTuple(Arrays.asList("{\"name\": \"value\", \"nestedJson\": {\"json\": \"ihazit\"}}"));
    Map<String, String> result = udf_.exec(input);
    assertTrue("Nested Json should just return as a String", result.get("nestedJson") instanceof String);
  }
}
