package com.twitter.elephantbird.mapred.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat;

/**
 * This is a copy of {@link LzoJsonInputFormat}.
 *
 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style
 * which is deprecated but still required in places.  Streaming, for example,
 * does a check that the given input format is a descendant of
 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
 * from the new API fails.
 */
@SuppressWarnings("deprecation")
public class DeprecatedLzoJsonInputFormat extends DeprecatedLzoInputFormat<LongWritable, MapWritable>{
  @Override
  public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new DeprecatedLzoJsonRecordReader(conf, (FileSplit)split);
  }
}
