package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * InputFormat which uses {@link RawSequenceFileRecordReader} to read keys and values from
 * SequenceFiles as {@link DataInputBuffer} instances.
 *
 * @author Andy Schlaikjer
 */
public class RawSequenceFileInputFormat extends
    SequenceFileInputFormat<DataInputBuffer, DataInputBuffer> {
  @Override
  public RecordReader<DataInputBuffer, DataInputBuffer> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new RawSequenceFileRecordReader();
  }
}
