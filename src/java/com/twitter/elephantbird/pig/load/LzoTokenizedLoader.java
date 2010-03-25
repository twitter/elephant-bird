package com.twitter.elephantbird.pig.load;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.twitter.elephantbird.pig.util.PigTokenHelper;
import org.apache.pig.SamplableLoader;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A load function that parses a line of input into fields using a delimiter to set the fields. The
 * delimiter is given as a single character, \t, or \x___ or slash u___.
 */
public class LzoTokenizedLoader extends LzoBaseLoadFunc implements SamplableLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LzoTokenizedLoader.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();

  private ByteArrayOutputStream buffer_ = new ByteArrayOutputStream(4096);
  private final byte recordDel_ = PigTokenHelper.DEFAULT_RECORD_DELIMITER;
  private byte fieldDel_ = PigTokenHelper.DEFAULT_FIELD_DELIMITER;
  private ArrayList<Object> protoTuple_ = null;
  private Byte prevByte_ = null;

  /**
   * Constructs a Pig loader that uses specified character as a field delimiter.
   *
   * @param delimiter the single byte character that is used to separate fields.
   *        examples are ':', '\t', or '\u0001'
   */
  public LzoTokenizedLoader(String delimiter) {
    LOG.info("LzoTokenizedLoader with given delimiter [" + delimiter + "]");

    // Store the constructor args so that individual slicers can recreate them.
    setLoaderSpec(getClass(), new String[] { delimiter });
    fieldDel_ = PigTokenHelper.evaluateDelimiter(delimiter);
  }

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first record of each split and count on a different
    // instance to read it.  The only split this doesn't work for is the first.
    if (!atFirstRecord) {
      getNext();
    }
  }

  /**
   * Break the next line into a delimited set of fields.  Note that this can and
   * does result in tuples with different numbers of fields, which is tracked by
   * a counter.
   */
  public Tuple getNext() throws IOException {
    if (!verifyStream()) {
      return null;
    }

    Tuple t = null;
    buffer_.reset();
    while (true) {
      // BufferedPositionedInputStream is buffered, so no need to buffer.
      int b = is_.read();
      prevByte_ = (byte)b;
      if (prevByte_ == fieldDel_) {
        readField();
      } else if (prevByte_ == recordDel_) {
        readField();
        t =  tupleFactory_.newTupleNoCopy(protoTuple_);
        protoTuple_ = null;
        break;
      } else if (b == -1) {
        // hit end of file
        break;
      } else {
        buffer_.write(b);
      }
    }

    if (t != null) {
      // Increment the per-tuple-size counters.
      incrCounter(getClass().getName(), getCounterName(t.size()), 1L);
    }

    return t;
  }

  /**
   * For SamplableLoader, return the stream position.
   */
  public long getPosition() throws IOException {
    return is_.getPosition();
  }

  /**
   * For SamplableLoader.  Skip almost all the way, see if we're at the end, then skip the last byte.
   */
  public long skip(long n) throws IOException {
    long skipped = is_.skip(n - 1);
    prevByte_ = (byte)is_.read();
    if(prevByte_ == -1) // End of stream.
      return skipped;
    else
      return skipped + 1;
  }

  /**
   * For SamplableLoader, return a tuple at the given position.
   */
  public Tuple getSampledTuple() throws IOException {
    if(prevByte_ == null || prevByte_ == recordDel_) {
      // prevByte = null when this is called for the first time, in that case bindTo would have already
      // called getNext() if it was required.
      return getNext();
    } else {
      // We are in middle of record. So, we skip this and return the next one.
      getNext();
      return getNext();
    }
  }

  /**
   * Construct a field from the input buffer, which at this point should be
   * pointing at a single token.
   */
  private void readField() {
    if (protoTuple_ == null) {
      protoTuple_ = new ArrayList<Object>();
    }

    if (buffer_.size() == 0) {
      // NULL value
      protoTuple_.add(null);
    } else {
      byte[] array = buffer_.toByteArray();

      if (array.length == 0) {
        protoTuple_.add(null);
      } else {
        protoTuple_.add(new DataByteArray(array));
      }
    }
    buffer_.reset();
  }

  /**
   * An internal helper function to get a counter name.
   * @param i the number of fields
   */
  private String getCounterName(Integer i) {
    return "Tuples with " + i + " fields";
  }

  /**
   * LzoTokenizedLoaders are determined by their field delimiter.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LzoTokenizedLoader) {
      LzoTokenizedLoader other = (LzoTokenizedLoader)obj;
      return fieldDel_ == other.fieldDel_;
    }
    return false;
  }
}
