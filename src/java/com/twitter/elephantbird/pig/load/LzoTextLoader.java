package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load the LZO file line by line, passing each line as a single-field Tuple to Pig.
 */
public class LzoTextLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoJsonLoader.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  protected enum LzoTextLoaderCounters { LinesRead }

  public LzoTextLoader() {
    LOG.info("LzoTextLoader creation");
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

    String line = is_.readLine(UTF8, RECORD_DELIMITER);
    Tuple t = null;
    if (line != null) {
      incrCounter(LzoTextLoaderCounters.LinesRead, 1L);
      t = tupleFactory_.newTuple(new DataByteArray(line.getBytes()));
    }

    return t;
  }
}
