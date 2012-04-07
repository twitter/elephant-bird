package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

public class LzoJsonLoader extends JsonLoader {
  
  /**
   * Constructor. Construct a LzoJsonLoader LoadFunc to load.
   * @param optString Loader options. For available options,
   * see {@link JsonLoader#JsonLoader(String, String)}
   */
  public LzoJsonLoader(String optString) {
    super(LzoTextInputFormat.class.getName(), optString);
  }
  
  public LzoJsonLoader() {
    // defaults to no options
    this("");
  }
}
