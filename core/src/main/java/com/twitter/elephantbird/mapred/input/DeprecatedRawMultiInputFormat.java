package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.RawMultiInputFormat;

/**
 * mapred version of {@link com.twitter.elephantbird.mapreduce.input.RawMultiInputFormat}
 */
public class DeprecatedRawMultiInputFormat extends DeprecatedInputFormatWrapper {

  @SuppressWarnings("unchecked")
  public DeprecatedRawMultiInputFormat() {
    super(new RawMultiInputFormat());
  }
}
