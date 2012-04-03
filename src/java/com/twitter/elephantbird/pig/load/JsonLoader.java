package com.twitter.elephantbird.pig.load;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Decodes each line as JSON passes the resulting map of values
 * to Pig as a single-element tuple.
 */
public class JsonLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(JsonLoader.class);
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final BagFactory bagFactory = DefaultBagFactory.getInstance();

  private final JSONParser jsonParser = new JSONParser();

  private enum JsonLoaderCounters {
    LinesRead,
    LinesJsonDecoded,
    LinesParseError,
    LinesParseErrorBadNumber
  }

  private String inputFormatClassName;
  private boolean isNestedLoadEnabled = false;

  public JsonLoader() {
    this(TextInputFormat.class.getName());
  }

  public JsonLoader(String inputFormatClassName) {
    this.inputFormatClassName = inputFormatClassName;
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader == null) {
      return null;
    }
    isNestedLoadEnabled = "true".equals(jobConf.get("jsonLoader.nestedLoad.enabled"));
    try {
      while (reader.nextKeyValue()) {
        Text value = (Text) reader.getCurrentValue();
        incrCounter(JsonLoaderCounters.LinesRead, 1L);
        Tuple t = parseStringToTuple(value.toString());
        if (t != null) {
          incrCounter(JsonLoaderCounters.LinesJsonDecoded, 1L);
          return t;
        }
      }
      return null;

    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    }
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    try {
      return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
    } catch (InstantiationException e) {
      throw new IOException("Failed creating input format " + inputFormatClassName, e);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed creating input format " + inputFormatClassName, e);
    }
  }

  protected Tuple parseStringToTuple(String line) {
    try {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(line);
      if (jsonObj != null) {
        return tupleFactory.newTuple(walkJson(jsonObj));
      } else {
        // JSONParser#parse(String) may return a null reference, e.g. when
        // the input parameter is the string "null".  A single line with
        // "null" is not valid JSON though.
        LOG.warn("Could not json-decode string: " + line);
        incrCounter(JsonLoaderCounters.LinesParseError, 1L);
        return null;
      }
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      incrCounter(JsonLoaderCounters.LinesParseError, 1L);
      return null;
    } catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      incrCounter(JsonLoaderCounters.LinesParseErrorBadNumber, 1L);
      return null;
    } catch (ClassCastException e) {
      LOG.warn("Could not convert to Json Object: " + line, e);
      incrCounter(JsonLoaderCounters.LinesParseError, 1L);
      return null;
    }
  }

  private Object wrap(Object value) {
    
    if (isNestedLoadEnabled && value instanceof JSONObject) {
      return walkJson((JSONObject) value);
    }  else if (isNestedLoadEnabled && value instanceof JSONArray) {
      
      JSONArray a = (JSONArray) value;
      DataBag mapValue = bagFactory.newDefaultBag();
      for (int i=0; i<a.size(); i++) {
        Tuple t = tupleFactory.newTuple(wrap(a.get(i)));
        mapValue.add(t);
      }
      return mapValue;
      
    } else {
      return value != null ? value.toString() : null;
    }
  }

  private Map<String,Object> walkJson(JSONObject jsonObj) {
    Map<String,Object> v = Maps.newHashMap();
    for (Object key: jsonObj.keySet()) {
      v.put(key.toString(), wrap(jsonObj.get(key)));
    }
    return v;
  }
  
}
