package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.combined.CombineLzoFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DeprecatedCombineLzoTextInputFormat extends DeprecatedFileInputFormatWrapper<LongWritable, Text> {
  public DeprecatedCombineLzoTextInputFormat() {
        super(new LzoCombineFileInputFormat());
    }
}
