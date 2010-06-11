package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load the LZO file line by line, decoding each line as JSON and passing the
 * resulting map of values to Pig as a single-element tuple.
 *
 * WARNING: Currently does not handle multi-line JSON well, if at all.
 * WARNING: Up through 0.6, Pig does not handle complex values in maps well,
 * so use only with simple (native datatype) values -- not bags, arrays, etc.
 */

public class LzoJsonLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoJsonLoader.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  private final JSONParser jsonParser_ = new JSONParser();

  protected enum LzoJsonLoaderCounters { LinesRead, LinesJsonDecoded, LinesParseError, LinesParseErrorBadNumber }

  public LzoJsonLoader() {
    LOG.info("LzoJsonLoader creation");
  }

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first record of each split and count on a different
    // instance to read it.  The only split this doesn't work for is the first.
    if (!atFirstRecord) {
      getNext();
    }
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  public Tuple getNext() throws IOException {
    if (!verifyStream()) {
      return null;
    }

    String line;
    while ((line = is_.readLine(UTF8, RECORD_DELIMITER)) != null) {
      incrCounter(LzoJsonLoaderCounters.LinesRead, 1L);

      Tuple t = parseStringToTuple(line);
      if (t != null) {
        incrCounter(LzoJsonLoaderCounters.LinesJsonDecoded, 1L);
        return t;
      }
    }

    return null;
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
