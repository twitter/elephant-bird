package com.twitter.elephantbird.pig.load;

import com.twitter.elephantbird.pig.store.LzoPigStorage;

/**
 * Same as {@link LzoPigStorage}.
 */
public class LzoTokenizedLoader extends LzoPigStorage {

  public LzoTokenizedLoader() {
    super();
  }

  public LzoTokenizedLoader(String delimiter) {
    super(delimiter);
  }
}
