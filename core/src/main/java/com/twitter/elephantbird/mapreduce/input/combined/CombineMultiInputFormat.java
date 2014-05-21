package com.twitter.elephantbird.mapreduce.input.combined;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import org.apache.hadoop.io.LongWritable;

public class CombineMultiInputFormat<M> extends DelegateCombineFileInputFormat<LongWritable, BinaryWritable<M>> {
  public CombineMultiInputFormat() {
    super(new MultiInputFormat());
  }
}