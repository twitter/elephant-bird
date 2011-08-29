package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;

/**
 * Utility to simplify iteration over Tuples loaded by some {@link LoadFunc}.
 *
 * @author Andy Schlaikjer
 */
public class LoadFuncTupleIterator implements Iterator<Tuple> {
  private final LoadFunc loadFunc;
  private boolean hasNextCalled;
  private Tuple tuple;

  public LoadFuncTupleIterator(LoadFunc loadFunc) {
    super();
    this.loadFunc = loadFunc;
  }

  @Override
  public boolean hasNext() {
    if (!hasNextCalled) {
      hasNextCalled = true;
      if (tuple == null) {
        try {
          tuple = loadFunc.getNext();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return tuple != null;
  }

  @Override
  public Tuple next() {
    if (!hasNext())
      throw new NoSuchElementException();
    Tuple next = tuple;
    hasNextCalled = false;
    tuple = null;
    return next;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
