package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.mapreduce.input.LzoW3CLogInputFormat;
import com.twitter.elephantbird.mapreduce.input.LzoW3CLogRecordReader;
import com.twitter.elephantbird.pig.load.LzoJsonLoader.LzoJsonLoaderCounters;
import com.twitter.elephantbird.util.W3CLogParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Load w3c log LZO file line by line, passing each line as a single-field Tuple to Pig.
 */
public class LzoW3CLogLoader extends LzoBaseLoadFunc {
  protected static final Logger LOG = LoggerFactory.getLogger(LzoW3CLogLoader.class);

  protected static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  protected W3CLogParser w3cParser_ = null;
  protected enum LzoW3CLogLoaderCounters { LinesRead, LinesW3CDecoded };

  public LzoW3CLogLoader(String fileURI) throws IOException {
    LOG.info("Initialize LzoW3CLogLoader from " + fileURI);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(fileURI), conf);
    InputStream fieldDefIs = fs.open(new Path(fileURI));
    w3cParser_ = new W3CLogParser(fieldDefIs);
    fieldDefIs.close();
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
	  MapWritable value_;
	  String line;
	  try {
		  while( is_.nextKeyValue() && (value_ = (MapWritable)is_.getCurrentValue()) != null) {
			  try {
			      Map<String, String> values = Maps.newHashMap();
			      
			      for (Object key: value_.keySet()) {
			        Object value = value_.get(key);
			        values.put(key.toString(), value != null ? value.toString() : null);
			      }
				  incrCounter(LzoW3CLogLoaderCounters.LinesRead, 1L);
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
	  } catch (InterruptedException e) {
		  int errCode = 6018;
		  String errMsg = "Error while reading input";
		  throw new ExecException(errMsg, errCode,
				  PigException.REMOTE_ENVIRONMENT, e);
	  }
	  return null;
  }

  protected Tuple parseStringToTuple(String line) {
    try {
      return tupleFactory_.newTuple(w3cParser_.parse(line));
    } catch (IOException e) {
      // Remove this for now because our logs have a lot of unmatched records
//      LOG.warn("Could not w3c-decode string: " + line, e);
      return null;
    }
  }

  @Override
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return new Schema(new FieldSchema("data", DataType.MAP));
  }
  public void setLocation(String location, Job job)
  throws IOException {
	  FileInputFormat.setInputPaths(job, location);
  }
  public InputFormat getInputFormat() {
      return new LzoW3CLogInputFormat() {
		
		@Override
		public RecordReader<LongWritable, MapWritable> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new LzoW3CLogRecordReader() {
				
				@Override
				protected String getFieldDefinitionFile() {
					// TODO Auto-generated method stub
					return null;
				}
			};
		}
	};
  }

  public void prepareToRead(RecordReader reader, PigSplit split) {
	  is_ = (LzoW3CLogRecordReader)reader;
      
      
  }
}
