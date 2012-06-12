package com.twitter.elephantbird.mapred.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import com.twitter.elephantbird.mapreduce.input.LzoJsonInputFormat;

/**
 * mapred version of {@link LzoJsonInputFormat}.
 */
public class DeprecatedLzoJsonInputFormat extends DeprecatedInputFormatWrapper<LongWritable, MapWritable>{
  public DeprecatedLzoJsonInputFormat() {
    super(new LzoJsonInputFormat());
  }
}
