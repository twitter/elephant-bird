package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Base input format for Thrift and Protobuf RCFile input formats. <br>
 * contains a few common common utility methods.
 */
public abstract class RCFileBaseInputFormat extends MapReduceInputFormatWrapper<LongWritable, Writable> {

  /** internal, for MR use only. */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public RCFileBaseInputFormat() {
    super(new RCFileInputFormat());
  }

  /**
   * returns super.createRecordReader(split, taskAttempt). This is useful when
   * a sub class has its own their own wrapper over the base recordreader.
   */
  protected final RecordReader<LongWritable, Writable>
  createUnwrappedRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
          throws IOException, InterruptedException {
    return super.createRecordReader(split, taskAttempt);
  }

}
