package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.load.LzoJsonLoader.LzoJsonLoaderCounters;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * A basic Json Loader. Totally subject to change, this is mostly a cut and paste job.
 */
public class JsonLoader extends PigStorage {

  private static final Logger LOG = LoggerFactory.getLogger(JsonLoader.class);
  @SuppressWarnings("rawtypes")
  protected RecordReader reader_;

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();

  private final JSONParser jsonParser_ = new JSONParser();

  protected enum JsonLoaderCounters { LinesRead, LinesJsonDecoded, LinesParseError, LinesParseErrorBadNumber }

  // Making accessing Hadoop counters from Pig slightly more convenient.
  private final PigCounterHelper counterHelper_ = new PigCounterHelper();

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader_ == null) {
      return null;
    }
    try {
      while (reader_.nextKeyValue()) {
        Text value_ = (Text)reader_.getCurrentValue();
        incrCounter(LzoJsonLoaderCounters.LinesRead, 1L);
        Tuple t = parseStringToTuple(value_.toString());
        if (t != null) {
          incrCounter(LzoJsonLoaderCounters.LinesJsonDecoded, 1L);
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
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
      this.reader_ = reader;
  }

  // We could implement JsonInputFormat and proxy to it, and maybe that'd be worthwhile,
  // but taking the cheap shortcut for now.
  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() {
      return new TextInputFormat();
  }

  /**
   * A convenience function for working with Hadoop counter objects from load functions.  The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(Enum<?> key, long incr) {
    counterHelper_.incrCounter(key, incr);
  }

  protected Tuple parseStringToTuple(String line) {
    try {
      Map<String, String> values = Maps.newHashMap();
      JSONObject jsonObj = (JSONObject)jsonParser_.parse(line);
      for (Object key: jsonObj.keySet()) {
        Object value = jsonObj.get(key);
        values.put(key.toString(), value != null ? value.toString() : null);
      }
      return tupleFactory_.newTuple(values);
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
