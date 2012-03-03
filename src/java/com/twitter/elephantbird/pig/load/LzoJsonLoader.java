package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

public class LzoJsonLoader extends JsonLoader {
  public LzoJsonLoader() {
    super(LzoTextInputFormat.class.getName());
  }
}
