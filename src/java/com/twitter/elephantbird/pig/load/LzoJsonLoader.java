package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.mapreduce.input.LzoInputFormat;

/**
 * @deprecated Replaced by {@link JsonLoader}
 */
@Deprecated
public class LzoJsonLoader extends JsonLoader {

  public LzoJsonLoader() {
    super(LzoInputFormat.class.getName());
  }
}
