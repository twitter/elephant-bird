package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.mapreduce.input.LzoW3CLogInputFormat;
import com.twitter.elephantbird.mapreduce.input.LzoW3CLogRecordReader;


/**
 * Load w3c log LZO file line by line, passing each line as a single-field Tuple to Pig.
 */
public class LzoW3CLogLoader extends LzoBaseLoadFunc implements LoadMetadata {
  protected static final Logger LOG = LoggerFactory.getLogger(LzoW3CLogLoader.class);

  protected static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  protected final String fileURI;
  protected enum LzoW3CLogLoaderCounters { LinesW3CDecoded, UnparseableLines};

  /**
   * Constructor.
   * @param fileURI path to HDFS file that contains the CRC to column list mappings, one per line.
   * @throws IOException
   */
  public LzoW3CLogLoader(String fileURI) throws IOException {
    LOG.debug("Initialize LzoW3CLogLoader from " + fileURI);
    this.fileURI = fileURI;
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    LzoW3CLogRecordReader reader = (LzoW3CLogRecordReader) reader_;
    if (reader == null) {
      return null;
    }
    MapWritable value_;
    try {
      if ( reader.nextKeyValue() && (value_ = reader.getCurrentValue()) != null) {
          Map<String, String> values = Maps.newHashMap();

          for (Writable key: value_.keySet()) {
            Writable value = value_.get(key);
            values.put(key.toString(), value != null ? value.toString() : null);
          }
          incrCounter(LzoW3CLogLoaderCounters.LinesW3CDecoded, 1L);
          incrCounter(LzoW3CLogLoaderCounters.UnparseableLines, reader.getBadRecordsSkipped());
          return tupleFactory_.newTuple(values);
      }
    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    }
    return null;
  }

  @Override
  public void setLocation(String location, Job job)
  throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() {
    return LzoW3CLogInputFormat.newInstance(fileURI);
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
    return null;
  }

  @Override
  public ResourceSchema getSchema(String arg0, Job arg1) throws IOException {
    return new ResourceSchema(new Schema(new FieldSchema("data", DataType.MAP)));
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
    return null;
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public void setPartitionFilter(Expression arg0) throws IOException {

  }
}
