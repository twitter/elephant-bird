package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * Transforms a Json string into a Pig map whose value type is chararray. Only goes one level deep;
 * All input map values are converted to strings via {@link Object#toString()}.
 */
public class JsonStringToMap extends EvalFunc<Map<String, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonStringToMap.class);
  private final JSONParser jsonParser = new JSONParser();
  private final PigCounterHelper counterHelper = new PigCounterHelper();

  @Override
  public Schema outputSchema(Schema input) {
    try {
      return Utils.getSchemaFromString("json: [chararray]");
    } catch (ParserException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> exec(Tuple input) throws IOException {
    try {
      // Verify the input is valid, logging to a Hadoop counter if not.
      if (input == null || input.size() < 1) {
        throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
      }

      if (input.get(0) == null) {
        counterHelper.incrCounter(getClass().getName(), "NullJsonString", 1L);
        return null;
      }

      String jsonLiteral = (String) input.get(0);
      return parseStringToMap(jsonLiteral);
    } catch (ExecException e) {
      LOG.warn("Error in " + getClass() + " with input " + input, e);
      throw new IOException(e);
    }
  }

  protected Map<String, String> parseStringToMap(String line) {
    try {
      Map<String, String> values = Maps.newHashMap();
      JSONObject jsonObj = (JSONObject) jsonParser.parse(line);
      for (Object key : jsonObj.keySet()) {
        Object value = jsonObj.get(key);
        values.put(key.toString(), value != null ? value.toString() : null);
      }
      return values;
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      return null;
    } catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      return null;
    }
  }

}
