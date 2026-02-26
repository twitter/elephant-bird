package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

public class LzoJsonLoader extends JsonLoader {
  
  /**
   * Constructor. Construct a LzoJsonLoader LoadFunc to load.
   * @param optString Loader options. For available options,
   * see {@link JsonLoader#JsonLoader(String)}.
   * Notice that the -inputFormat option is overridden.
   */
  public LzoJsonLoader(String optString) {
    super(optString);
    this.setInputFormatClassName(LzoTextInputFormat.class.getName());
  }
  
  public LzoJsonLoader() {
    // defaults to no options
    this("");
  }
}
