package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

public class LzoJsonLoader extends JsonLoader {
  
  public LzoJsonLoader(String fieldsSpec) {
    super(LzoTextInputFormat.class.getName(), fieldsSpec);
  }
  
  public LzoJsonLoader() {
    // defaults to no fields specification
    this(null);
  }
}
