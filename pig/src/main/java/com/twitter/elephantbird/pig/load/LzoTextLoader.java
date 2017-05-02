package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.pig.store.LzoPigStorage;

/**
 * Load the LZO file line by line, passing each line as a single-field Tuple to Pig.
 */
public class LzoTextLoader extends LzoPigStorage {

  public LzoTextLoader() {
    super("\n");
  }
}
