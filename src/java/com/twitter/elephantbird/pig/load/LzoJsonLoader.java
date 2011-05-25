package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat;

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
  protected enum LzoJsonLoaderCounters { LinesRead, LinesJsonDecoded, LinesParseError, LinesParseErrorBadNumber }

  public LzoJsonLoader() {}

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
	      MapWritable value_ = (MapWritable)reader_.getCurrentValue();
	      incrCounter(LzoJsonLoaderCounters.LinesRead, 1L);
	      Tuple t = parseStringToTuple(value_);
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

  protected Tuple parseStringToTuple(MapWritable value_) {
    try {
      Map<String, String> values = Maps.newHashMap();

      for (Object key: value_.keySet()) {
        Object value = value_.get(key);
        values.put(key.toString(), value != null ? value.toString() : null);
      }
      return tupleFactory_.newTuple(values);
    }catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + value_.toString(), e);
      incrCounter(LzoJsonLoaderCounters.LinesParseErrorBadNumber, 1L);
      return null;
    } catch (ClassCastException e) {
      LOG.warn("Could not convert to Json Object: " + value_.toString(), e);
      incrCounter(LzoJsonLoaderCounters.LinesParseError, 1L);
      return null;
    }
  }

  @Override
  public void setLocation(String location, Job job)
  throws IOException {
	  FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public InputFormat getInputFormat() {
      return new LzoJsonInputFormat();
  }

}
