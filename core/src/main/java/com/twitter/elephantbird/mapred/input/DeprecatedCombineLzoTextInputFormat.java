package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.combined.CombineLzoFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This is a mapred api version of {@link CombineLzoFileInputFormat},
 * provided primarily for use with scalding via cascading.
 *
 * @author Jonathan Coveney
 */
public class DeprecatedCombineLzoTextInputFormat extends DeprecatedFileInputFormatWrapper<LongWritable, Text> {
  public DeprecatedCombineLzoTextInputFormat() {
        super(new CombineLzoFileInputFormat());
    }
}
