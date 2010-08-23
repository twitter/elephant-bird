package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoLineRecordReader;
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
	  Tuple t = null;
	  try {
		  Object line = is_.getCurrentValue();
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
  public void setLocation(String location, Job job)
  throws IOException {
	  FileInputFormat.setInputPaths(job, location);
  }
  public InputFormat getInputFormat() {
	  return new LzoTextInputFormat();
  }

  public void prepareToRead(RecordReader reader, PigSplit split)  throws IOException{
	  is_ = (LzoLineRecordReader)reader;
	  
  }
}
