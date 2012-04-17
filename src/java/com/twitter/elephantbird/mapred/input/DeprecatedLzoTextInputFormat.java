package com.twitter.elephantbird.mapred.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * mapred version of {@link LzoTextInputFormat}.
 */
public class DeprecatedLzoTextInputFormat extends DeprecatedInputFormatWrapper<LongWritable, Text> {
  public DeprecatedLzoTextInputFormat() {
    super(new LzoTextInputFormat());
  }
}
