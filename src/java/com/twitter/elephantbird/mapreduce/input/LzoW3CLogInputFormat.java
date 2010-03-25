package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

/**
 * An input format for LZO-encoded W3C-style log files.  The W3C style has lines that read
 * hash value1 value2 value3 ... valueN
 * with a corresponding lookup file
 * hash field1 field2 field3 ... fieldN
 * where fieldK is the name of the metric valueK represents.  The hash is a CRC of all the
 * field names, and adding or removing metrics results in a new hash at the beginning of the
 * associated line.  This class expects there to be a filename (in HDFS) that contains all
 * the hash/field-name lines used in the course of writing the data, and uses those as a look-aside
 * to parse W3C log lines into a hash map.  To use the class, derive from LzoW3CLogInputFormat
 * and override createRecordReader to return a LzoW3CLogReaderReader-derived object.
 */

public abstract class LzoW3CLogInputFormat extends LzoInputFormat<LongWritable, MapWritable> {
  /**
   * A placeholder class to remind you to override createRecordReader with one that returns your
   * LzoW3CLogRecordReader-derived class.  All that class has to do is override the
   * getFieldDefinitionFile method; an inline example is below.
   */
  /*
  @Override
  public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    return new LzoW3CLogRecordReader() {
      @Override
      protected String getFieldDefinitionFile() {
        return "/path/to/my/w3c/field/definition/file";
      }
    };
  }
  */
}
