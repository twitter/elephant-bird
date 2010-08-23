package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.twitter.elephantbird.mapreduce.input.LzoLineRecordReader;
import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * Serves as the base class for all regex-based lzo loading functions.  Each deriving
 * class just needs to implement getPattern in some way.
 */

public abstract class LzoBaseRegexLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseRegexLoader.class);

  protected static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  protected static final Charset UTF8 = Charset.forName("UTF-8");
  protected static final byte RECORD_DELIMITER = (byte)'\n';
  // This loader tracks the number of matched and unmatched lines as Hadoop counters.
  protected enum LzoBaseRegexLoaderCounters { MatchedRegexLines, UnmatchedRegexLines }

  public LzoBaseRegexLoader() {
    LOG.info("LzoBaseRegexLoader created.");
  }

  /**
   * The regex pattern must be filled out by the inheritor.
   */
  public abstract Pattern getPattern();

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first record of each split and count on a different
    // instance to read it.  The only split this doesn't work for is the first.
    if (!atFirstRecord) {
      getNext();
    }
  }

  /**
   * Read the file line by line, returning lines the match the regex in Tuples
   * based on the regex match groups.
   */
  public Tuple getNext() throws IOException {
	  if (!verifyStream()) {
		  return null;
	  }

	  Pattern pattern = getPattern();
	  Matcher matcher = pattern.matcher("");
	  Object lineObj;
	  String line;
	  Tuple t = null;
	  // Read lines until a match is found, making sure there's no reading past the
	  // end of the assigned byte range.
	  try {
		  while (is_.nextKeyValue()) {

			  lineObj = is_.getCurrentValue();

			  if (lineObj == null) {
				  break;
			  }
			  line = lineObj.toString();
			  matcher = matcher.reset(line);
			  // Increment counters for the number of matched and unmatched lines.
			  if (matcher.find()) {

				  incrCounter(LzoBaseRegexLoaderCounters.MatchedRegexLines, 1L);
				  t = tupleFactory_.newTuple(matcher.groupCount());
				  for (int i = 1; i <= matcher.groupCount(); i++) {
					  if(matcher.group(i) != null) {
						  t.set(i - 1, matcher.group(i));
					  } else {
						  t.set(i - 1, "");
					  }
				  }
				  break;
			  } else {
				  incrCounter(LzoBaseRegexLoaderCounters.UnmatchedRegexLines, 1L);
				  // TODO: stop doing this, as it can slow down the job.
				  LOG.debug("No match for line " + line);
			  }

			  // If the read has walked beyond the end of the split, move on.

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

  public void prepareToRead(RecordReader reader, PigSplit split) {
	  is_ = (LzoLineRecordReader)reader;
      
      
  }
}
