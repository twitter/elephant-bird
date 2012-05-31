package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.output.LzoTextOutputFormat;

/**
 * mapred version of {@link LzoTextOutputFormat}.
 */
public class DeprecatedLzoTextOutputFormat<K, V>
               extends DeprecatedOutputFormatWrapper<K, V> {

  public DeprecatedLzoTextOutputFormat() {
    super(new LzoTextOutputFormat<K, V>());
  }
}
