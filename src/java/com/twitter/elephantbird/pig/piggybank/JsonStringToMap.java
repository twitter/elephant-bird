package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * <p>Transforms a Json string into a Pig map.<br>
 * Only goes 1 level deep -- all value representations are their toString() representations.</p>
 */
@SuppressWarnings("rawtypes")
public class JsonStringToMap extends EvalFunc<Map> {
  private static final Logger LOG = LogManager.getLogger(JsonStringToMap.class);
  private final JSONParser jsonParser = new JSONParser();
  private final PigCounterHelper counterHelper = new PigCounterHelper();

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
