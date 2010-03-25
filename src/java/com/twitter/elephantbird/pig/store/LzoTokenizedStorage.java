package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

import com.twitter.elephantbird.pig.util.PigTokenHelper;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage class to store the ouput of each tuple in a delimited file
 * like PigStorage, but LZO compressed.
 */
public class LzoTokenizedStorage extends LzoBaseStoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoTokenizedStorage.class);

  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final byte recordDel_ = PigTokenHelper.DEFAULT_RECORD_DELIMITER;
  private byte fieldDel_ = PigTokenHelper.DEFAULT_FIELD_DELIMITER;

  /**
   * Constructs a Pig storage object that uses the specified character as a field delimiter.
   *
   * @param delimiter the single byte character that is used to separate fields.
   *        Examples are ':', '\t', or '\u0001'.
   */
  public LzoTokenizedStorage(String delimiter) {
    LOG.info("LzoTokenizedStorage with given delimiter [" + delimiter + "]");
    fieldDel_ = PigTokenHelper.evaluateDelimiter(delimiter);
  }

  /**
   * Write the tuple out by writing its fields one at a time, separated by the delimiter.
   * @param tuple the tuple to write.
   */
  public void putNext(Tuple tuple) throws IOException {
    // Must convert integer fields to string, and then to bytes.
    // Otherwise a DataOutputStream will convert directly from integer to bytes, rather
    // than using the string representation.
    int numElts = tuple.size();
    for (int i = 0; i < numElts; i++) {
      Object field;
      try {
        field = tuple.get(i);
      } catch (ExecException ee) {
        throw ee;
      }

      putField(field);

      if (i == numElts - 1) {
        // Last field in tuple.
        os_.write(recordDel_);
      } else {
        os_.write(fieldDel_);
      }
    }
  }

  /**
   * Put an individual field in the tuple.
   * @param field the object to store.
   */
  @SuppressWarnings("unchecked")
  private void putField(Object field) throws IOException {
    switch (DataType.findType(field)) {
    case DataType.NULL:
      break; // just leave it empty

    case DataType.BOOLEAN:
      os_.write(((Boolean)field).toString().getBytes());
      break;

    case DataType.INTEGER:
      os_.write(((Integer)field).toString().getBytes());
      break;

    case DataType.LONG:
      os_.write(((Long)field).toString().getBytes());
      break;

    case DataType.FLOAT:
      os_.write(((Float)field).toString().getBytes());
      break;

    case DataType.DOUBLE:
      os_.write(((Double)field).toString().getBytes());
      break;

    case DataType.BYTEARRAY: {
      byte[] b = ((DataByteArray)field).get();
      os_.write(b, 0, b.length);
      break;
    }
    case DataType.CHARARRAY:
      // oddly enough, writeBytes writes a string
      os_.write(((String)field).getBytes(UTF8));
      break;

    case DataType.MAP:
      boolean mapHasNext = false;
      Map<String, Object> m = (Map<String, Object>)field;
      os_.write(PigTokenHelper.MAP_BEGIN.getBytes(UTF8));
      for(String s: m.keySet()) {
        if(mapHasNext) {
          os_.write(fieldDel_);
        } else {
          mapHasNext = true;
        }
        putField(s);
        os_.write(PigTokenHelper.MAP_KV.getBytes(UTF8));
        putField(m.get(s));
      }
      os_.write(PigTokenHelper.MAP_END.getBytes(UTF8));
      break;

    case DataType.TUPLE:
      boolean tupleHasNext = false;
      Tuple t = (Tuple)field;
      os_.write(PigTokenHelper.TUPLE_BEGIN.getBytes(UTF8));
      for(int i = 0; i < t.size(); ++i) {
        if(tupleHasNext) {
          os_.write(fieldDel_);
        } else {
          tupleHasNext = true;
        }
        try {
          putField(t.get(i));
        } catch (ExecException ee) {
          throw ee;
        }
      }
      os_.write(PigTokenHelper.TUPLE_END.getBytes(UTF8));
      break;

    case DataType.BAG:
      boolean bagHasNext = false;
      os_.write(PigTokenHelper.BAG_BEGIN.getBytes(UTF8));
      Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
      while(tupleIter.hasNext()) {
        if(bagHasNext) {
          os_.write(fieldDel_);
        } else {
          bagHasNext = true;
        }
        putField(tupleIter.next());
      }
      os_.write(PigTokenHelper.BAG_END.getBytes(UTF8));
      break;

    default:
      int errCode = 2108;
      String msg = "Could not determine data type of field: " + field;
      throw new ExecException(msg, errCode, PigException.BUG);
    }
  }

  /**
   * Two LzoTokenizedStorage functions are equal if their delimiters are.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LzoTokenizedStorage) {
      LzoTokenizedStorage other = (LzoTokenizedStorage)obj;
      return fieldDel_ == other.fieldDel_;
    }
    return false;
  }
}

