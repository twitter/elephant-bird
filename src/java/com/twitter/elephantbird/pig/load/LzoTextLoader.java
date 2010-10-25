package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * Load the LZO file line by line, passing each line as a single-field Tuple to Pig.
 */
public class LzoTextLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoTextLoader.class);

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  protected enum LzoTextLoaderCounters { LinesRead }

  public LzoTextLoader() {
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
	  if (reader_ == null) {
		  return null;
	  }
	  Tuple t = null;
	  try {
		  Object line = reader_.getCurrentValue();
		  if (line != null) {
			  incrCounter(LzoTextLoaderCounters.LinesRead, 1L);
			  t = tupleFactory_.newTuple(new DataByteArray(line.toString().getBytes()));
		  }
	  } catch (InterruptedException e) {
		  int errCode = 6018;
		  String errMsg = "Error while reading input";
		  throw new ExecException(errMsg, errCode,
				  PigException.REMOTE_ENVIRONMENT, e);
	  }
	  return t;
  }

  @Override
  public InputFormat getInputFormat() {
	  return new LzoTextInputFormat();
  }

}
