package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;

/**
 * This is the base class for all base64 encoded, line-oriented
 * pig loaders for protocol buffers, Thrift etc.
 * Data is expected to be one base64 encoded serialized object per line.
 */

public abstract class LzoBinaryB64LinePigLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBinaryB64LinePigLoader.class);

  private BinaryConverter<Tuple> tupleConverter_ = null;
  private final Base64 base64_ = new Base64(0);

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  private Pair<String, String> linesRead;
  private Pair<String, String> recordsRead;
  private Pair<String, String> errors;

  /**
   * className is used for naming counters.
   */
  protected void init(String className, BinaryConverter<Tuple> tupleConverter) {
    tupleConverter_ = tupleConverter;
    String group = "LzoB64Lines of " + className;
    linesRead = new Pair<String, String>(group, "Lines Read");
    recordsRead = new Pair<String, String>(group, "Records Read");
    errors = new Pair<String, String>(group, "Errors");
  }

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first (mostly partial)
    // line of each split and count on a different
    // instance to read it. The only split this doesn't work for is the first.
    if (!atFirstRecord && verifyStream()) {
      is_.readLine(UTF8, RECORD_DELIMITER);
      // don't increment linesRead either.
    }
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  public Tuple getNext() throws IOException {
    String line;
    Tuple t = null;

    while (verifyStream() && (line = is_.readLine(UTF8, RECORD_DELIMITER)) != null) {
      incrCounter(linesRead, 1L);
      t = tupleConverter_.fromBytes(base64_.decode(line.getBytes(UTF8)));
      if (t != null) {
        incrCounter(recordsRead, 1L);
        break;
      } else {
        incrCounter(errors, 1L);
      }
    }

    return t;
  }
}
