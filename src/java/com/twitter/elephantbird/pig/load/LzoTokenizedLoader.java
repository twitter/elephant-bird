package com.twitter.elephantbird.pig.load;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import com.twitter.elephantbird.pig.util.PigTokenHelper;

/**
 * A load function that parses a line of input into fields using a delimiter to set the fields. The
 * delimiter is given as a single character, \t, or \x___ or slash u___.
 */
public class LzoTokenizedLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoTokenizedLoader.class);
  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();

  private final ByteArrayOutputStream buffer_ = new ByteArrayOutputStream(4096);
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
    if (delimiter.length() > 1) {
      LOG.error("Delimiter is not a single character. Cannot construct LzoTokenizedLoader.");
      throw new IllegalArgumentException();
    }
    // Store the constructor args so that individual slicers can recreate them.
    setLoaderSpec(getClass(), new String[] { delimiter });
    fieldDel_ = PigTokenHelper.evaluateDelimiter(delimiter);
  }

  /**
   * Break the next line into a delimited set of fields.  Note that this can and
   * does result in tuples with different numbers of fields, which is tracked by
   * a counter.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader_ == null) {
      return null;
    }

    try {
      if(!reader_.nextKeyValue()) {
        return null;
      }
    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    }

    Tuple t = null;
    buffer_.reset();
    try {
      Text line = (Text)reader_.getCurrentValue();
      if (line != null) {
        String lineStr = line.toString();
        int len = lineStr.length();
        for (int i= 0;i<len;i++) {
          int b = lineStr.charAt(i);
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
        readField();
        t =  tupleFactory_.newTupleNoCopy(protoTuple_);
        protoTuple_ = null;
      }
    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    }

    if (t != null) {
      // Increment the per-tuple-size counters.
      incrCounter(getClass().getName(), getCounterName(t.size()), 1L);
    }

    return t;
  }




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

  @Override
  public InputFormat getInputFormat() {
    return new LzoTextInputFormat();
  }

}
