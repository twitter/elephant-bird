package com.twitter.elephantbird.pig.load;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Decodes each line as JSON passes the resulting map of values
 * to Pig as a single-element tuple.
 */
public class JsonLoader extends LoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(JsonLoader.class);
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  private final JSONParser jsonParser = new JSONParser();

  private final PigCounterHelper counterHelper = new PigCounterHelper();

  private enum JsonLoaderCounters {
    LinesRead,
    LinesJsonDecoded,
    LinesParseError,
    LinesParseErrorBadNumber
  }

  private RecordReader reader;
  private String inputFormatClassName;
  private FileInputFormat inputFormat;

  public JsonLoader() {
    this(TextInputFormat.class.getName());
  }

  public JsonLoader(String inputFormatClassName) {
    this.inputFormatClassName = inputFormatClassName;
    try {
      this.inputFormat = (FileInputFormat) Class.forName(inputFormatClassName).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Failed creating input format " + inputFormatClassName, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed creating input format " + inputFormatClassName, e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed creating input format " + inputFormatClassName, e);
    }
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    try {
      Method m = inputFormat.getClass().getMethod("setInputPaths",
          new Class[]{Job.class, String.class});
      m.invoke(null, job, location);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed setting input paths on " + inputFormatClassName, e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Failed setting input paths on " + inputFormatClassName, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed setting input paths on " + inputFormatClassName, e);
    }
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
        counterHelper.incrCounter(JsonLoaderCounters.LinesRead, 1L);
        Tuple t = parseStringToTuple(value.toString());
        if (t != null) {
          counterHelper.incrCounter(JsonLoaderCounters.LinesJsonDecoded, 1L);
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
  public void prepareToRead(RecordReader reader, PigSplit split) {
    this.reader = reader;
  }

  @Override
  public InputFormat getInputFormat() {
    return inputFormat;
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
        counterHelper.incrCounter(JsonLoaderCounters.LinesParseError, 1L);
        return null;
      }
      return tupleFactory.newTuple(values);
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      counterHelper.incrCounter(JsonLoaderCounters.LinesParseError, 1L);
      return null;
    } catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      counterHelper.incrCounter(JsonLoaderCounters.LinesParseErrorBadNumber, 1L);
      return null;
    } catch (ClassCastException e) {
      LOG.warn("Could not convert to Json Object: " + line, e);
      counterHelper.incrCounter(JsonLoaderCounters.LinesParseError, 1L);
      return null;
    }
  }

}
