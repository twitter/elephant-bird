package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * A basic Json Loader. Totally subject to change, this is mostly a cut and paste job.
 */
public class JsonLoader extends PigStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LzoJsonLoader.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  private final JSONParser jsonParser_ = new JSONParser();

  protected enum LzoJsonLoaderCounters { LinesRead, LinesJsonDecoded, LinesParseError, LinesParseErrorBadNumber }

  // Making accessing Hadoop counters from Pig slightly more convenient.
  private final PigCounterHelper counterHelper_ = new PigCounterHelper();

  private long end_;

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (in == null || in.getPosition() > end_) {
      return null;
    }
    Text value = new Text();
    boolean notDone = in.next(value);
    if (!notDone) {
      return null;
    }
    incrCounter(LzoJsonLoaderCounters.LinesRead, 1L);

    Tuple t = parseStringToTuple(value.toString());
    if (t != null) {
      incrCounter(LzoJsonLoaderCounters.LinesJsonDecoded, 1L);
    }
    return t;
  }

  @Override
  public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
    super.bindTo(fileName, in, offset, end);
    // end is private in PigStorage
    this.end_ = end;
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
      incrCounter(LzoJsonLoaderCounters.LinesParseError, 1L);
      return null;
    } catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      incrCounter(LzoJsonLoaderCounters.LinesParseErrorBadNumber, 1L);
      return null;
    } catch (ClassCastException e) {
      LOG.warn("Could not convert to Json Object: " + line, e);
      incrCounter(LzoJsonLoaderCounters.LinesParseError, 1L);
      return null;
    }
  }

}
