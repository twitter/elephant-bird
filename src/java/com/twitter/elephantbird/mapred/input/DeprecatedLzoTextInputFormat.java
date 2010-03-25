package com.twitter.elephantbird.mapred.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A copy of the TextInputFormat class for use with LZO-encoded data.  Should be
 * identical to TextInputFormat in use.
 *
 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style
 * which is deprecated but still required in places.  Streaming, for example,
 * does a check that the given input format is a descendant of
 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
 * from the new API fails.
 */

@SuppressWarnings("deprecation")
public class DeprecatedLzoTextInputFormat extends DeprecatedLzoInputFormat<LongWritable, Text> {
  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new DeprecatedLzoLineRecordReader(conf, (FileSplit)split);
  }

}
