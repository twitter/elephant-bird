package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;

/**
 * Utility to simplify iteration over Tuples loaded by some {@link LoadFunc}.
 *
 * @author Andy Schlaikjer
 */
public class LoadFuncTupleIterator implements Iterator<Tuple> {
  private final LoadFunc loadFunc;
  private Tuple tuple;

  public LoadFuncTupleIterator(LoadFunc loadFunc) {
    super();
    this.loadFunc = loadFunc;
  }

  @Override
  public boolean hasNext() {
    if (tuple == null) {
      try {
        tuple = loadFunc.getNext();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (tuple != null) {
      return true;
    }
    return false;
  }

  @Override
  public Tuple next() {
    Tuple current = tuple;
    tuple = null;
    return current;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
