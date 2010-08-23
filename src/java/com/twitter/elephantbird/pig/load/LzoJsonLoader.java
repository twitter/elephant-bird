package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat;
import com.twitter.elephantbird.mapreduce.input.LzoJsonRecordReader;

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
	  try {

		  MapWritable value_ = (MapWritable)is_.getCurrentValue();
		  incrCounter(LzoJsonLoaderCounters.LinesRead, 1L);

		  Tuple t = parseStringToTuple(value_);
		  if (t != null) {
			  incrCounter(LzoJsonLoaderCounters.LinesJsonDecoded, 1L);
			  return t;
		  }
	  } catch (InterruptedException e) {
		  int errCode = 6018;
		  String errMsg = "Error while reading input";
		  throw new ExecException(errMsg, errCode,
				  PigException.REMOTE_ENVIRONMENT, e);
	  }


	  return null;
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
  
  public void setLocation(String location, Job job)
  throws IOException {
	  FileInputFormat.setInputPaths(job, location);
  }
  public InputFormat getInputFormat() {
      return new LzoJsonInputFormat();
  }

  public void prepareToRead(RecordReader reader, PigSplit split) {
	  is_ = (LzoJsonRecordReader)reader;
      
      
  }

}
