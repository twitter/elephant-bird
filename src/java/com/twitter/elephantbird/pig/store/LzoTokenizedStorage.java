package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.pig.util.PigTokenHelper;

/**
 * A storage class to store the ouput of each tuple in a delimited file
 * like PigStorage, but LZO compressed.
 */
public class LzoTokenizedStorage extends LzoBaseStoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoTokenizedStorage.class);

  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final char recordDel_ = PigTokenHelper.DEFAULT_RECORD_DELIMITER;
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

  @Override
  public void putNext(Tuple tuple) throws IOException {
    // Must convert integer fields to string, and then to bytes.
    // Otherwise a DataOutputStream will convert directly from integer to bytes, rather
    // than using the string representation.
    int numElts = tuple.size();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numElts; i++) {
      Object field;
      try {
        field = tuple.get(i);
      } catch (ExecException ee) {
        throw ee;
      }
      sb.append(fieldToString(field));

      if (i == numElts - 1) {
        // Last field in tuple.
        sb.append(recordDel_);
      } else {
        sb.append(fieldDel_);
      }
    }

    Text text = new Text(sb.toString());
    try {
        writer.write(NullWritable.get(), text);
    } catch (InterruptedException e) {
        throw new IOException(e);
    }

  }

  /**
   * Put an individual field in the tuple.
   * @param field the object to store.
   */
  @SuppressWarnings("unchecked")
  private String fieldToString(Object field) throws IOException {
    switch (DataType.findType(field)) {
    case DataType.NULL:
      return "";
    case DataType.BYTEARRAY: {
      byte[] b = ((DataByteArray)field).get();
      return String.valueOf(b);
    }
    case DataType.MAP:
      StringBuilder mapBuilder = new StringBuilder();
      boolean mapHasNext = false;
      Map<String, Object> m = (Map<String, Object>)field;
      mapBuilder.append(PigTokenHelper.MAP_BEGIN);
      for(String s: m.keySet()) {
        if(mapHasNext) {
          mapBuilder.append(fieldDel_);
        } else {
          mapHasNext = true;
        }
        mapBuilder.append(fieldToString(s));
        mapBuilder.append(PigTokenHelper.MAP_KV);
        mapBuilder.append(fieldToString(m.get(s)));
      }
      mapBuilder.append(PigTokenHelper.MAP_END);
      return mapBuilder.toString();

    case DataType.TUPLE:
      boolean tupleHasNext = false;
      Tuple t = (Tuple)field;
      StringBuilder tupleBuilder = new StringBuilder();
      tupleBuilder.append(PigTokenHelper.TUPLE_BEGIN);
      for(int i = 0; i < t.size(); ++i) {
        if(tupleHasNext) {
          tupleBuilder.append(fieldDel_);
        } else {
          tupleHasNext = true;
        }
        try {
          tupleBuilder.append(fieldToString(t.get(i)));
        } catch (ExecException ee) {
          throw ee;
        }
      }
      tupleBuilder.append(PigTokenHelper.TUPLE_END);
      return tupleBuilder.toString();

    case DataType.BAG:
      boolean bagHasNext = false;
      StringBuilder bagBuilder = new StringBuilder();
      bagBuilder.append(PigTokenHelper.BAG_BEGIN);
      Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
      while(tupleIter.hasNext()) {
        if(bagHasNext) {
          bagBuilder.append(fieldDel_);
        } else {
          bagHasNext = true;
        }
        bagBuilder.append(fieldToString(tupleIter.next()));
      }
      bagBuilder.append(PigTokenHelper.BAG_END);
      return bagBuilder.toString();

    default:
      return field.toString();
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

  @SuppressWarnings("rawtypes")
  @Override
  public OutputFormat getOutputFormat() {
      return new TextOutputFormat<WritableComparable, Text>();
  }

}

