package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.RawMultiInputFormat;

/**
 * mapred version of {@link RawMultiInputFormat}
 */
public class DeprecatedRawMultiInputFormat extends DeprecatedFileInputFormatWrapper {

  @SuppressWarnings("unchecked")
  public DeprecatedRawMultiInputFormat() {
    super(new RawMultiInputFormat());
  }
}
