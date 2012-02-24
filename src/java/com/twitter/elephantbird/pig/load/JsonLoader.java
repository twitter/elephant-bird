package com.twitter.elephantbird.pig.load;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
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

  private final JSONParser jsonParser = new JSONParser();

  private enum JsonLoaderCounters {
    LinesRead,
    LinesJsonDecoded,
    LinesParseError,
    LinesParseErrorBadNumber
  }

  private String inputFormatClassName;

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
      Map<String, String> values = Maps.newHashMap();
      JSONObject jsonObj = (JSONObject) jsonParser.parse(line);
      if (jsonObj != null) {
        for (Object key : jsonObj.keySet()) {
          Object value = jsonObj.get(key);
          values.put(key.toString(), value != null ? value.toString() : null);
        }
      } else {
        // JSONParser#parse(String) may return a null reference, e.g. when
        // the input parameter is the string "null".  A single line with
        // "null" is not valid JSON though.
        LOG.warn("Could not json-decode string: " + line);
        incrCounter(JsonLoaderCounters.LinesParseError, 1L);
        return null;
      }
      return tupleFactory.newTuple(values);
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

}
