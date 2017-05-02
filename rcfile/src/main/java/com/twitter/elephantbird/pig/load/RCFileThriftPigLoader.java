package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.input.RCFileThriftTupleInputFormat;
import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import com.twitter.elephantbird.util.RCFileUtil;

/**
 * Pig loader for Thrift object stored in RCFiles.
 */
public class RCFileThriftPigLoader extends ThriftPigLoader<TBase<?,?>> {

  private RCFileThriftTupleInputFormat.TupleReader thriftReader;

  /**
   * @param thriftClassName fully qualified name of the thrift class
   */
  public RCFileThriftPigLoader(String thriftClassName) {
    super(thriftClassName);
  }

  @Override @SuppressWarnings("unchecked")
  public InputFormat getInputFormat() throws IOException {
    return new RCFileThriftTupleInputFormat(typeRef);
  }

  @Override
  public Tuple getNext() throws IOException {
    if (thriftReader.isReadingUnknonwsColumn()) {
      //do normal bytes -> thrift -> tuple
      return super.getNext();
    }

    // otherwise bytes -> tuple
    try {
      if (thriftReader.nextKeyValue()) {
        return thriftReader.getCurrentTupleValue();
      }
    } catch (TException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return null;
  }

  @Override @SuppressWarnings("unchecked")
  public void prepareToRead(RecordReader reader, PigSplit split) {
    super.prepareToRead(reader, split);
    thriftReader = (RCFileThriftTupleInputFormat.TupleReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    super.setLocation(location, job);
    RCFileUtil.setRequiredFieldConf(HadoopCompat.getConfiguration(job),
                                    requiredFieldList);
  }

}

