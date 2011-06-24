package com.twitter.elephantbird.pig.store;

/**
 * Same as {@link LzoPigStorage}
 *
 * @deprecated use {@link LzoPigStorage} instead
 */
@Deprecated
public class LzoTokenizedStorage extends LzoPigStorage {

  public LzoTokenizedStorage() {
    super();
  }

  public LzoTokenizedStorage(String delimiter) {
    super(delimiter);
  }
}

