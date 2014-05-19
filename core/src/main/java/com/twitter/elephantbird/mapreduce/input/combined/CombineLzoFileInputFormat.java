package com.twitter.elephantbird.mapreduce.input.combined;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import com.twitter.elephantbird.mapreduce.input.combined.DelegateCombineFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This is a concrete wrapper for a
 * {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat}
 * version of {@link LzoTextInputFormat}.
 *
 * @author Jonathan Coveney
 */
public class CombineLzoFileInputFormat extends DelegateCombineFileInputFormat<LongWritable, Text> {
  public CombineLzoFileInputFormat() {
    super(new LzoTextInputFormat());
  }
}
