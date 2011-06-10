package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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

public class LzoW3CLogInputFormat extends LzoInputFormat<LongWritable, MapWritable> {

  @Override
  public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit arg0,
      TaskAttemptContext arg1) throws IOException, InterruptedException {
    throw new IllegalArgumentException("LzoW3CLogInputFormat must be initialized by calling newInstance(fieldFile)");
  }

  /**
   * Use this method to create valid instances of LzoW3CLogInputFormat
   * @param fieldDefinitionFile path to file in HDFS that contains the CRC hash to column mappings.
   * @return
   */
  public static LzoW3CLogInputFormat newInstance(final String fieldDefinitionFile) {
    return new LzoW3CLogInputFormat() {
      @Override
      public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit split,
          TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<LongWritable, MapWritable> reader = new LzoW3CLogRecordReader() {
          @Override
          protected String getFieldDefinitionFile() {
            return fieldDefinitionFile;
          }
        };
        reader.initialize(split, context);
        return reader;
      }
    };
  }
}
