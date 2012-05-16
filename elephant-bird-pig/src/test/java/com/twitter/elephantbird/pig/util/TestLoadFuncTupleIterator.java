package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LoadFuncTupleIterator}.
 *
 * @author Andy Schlaikjer
 */
public class TestLoadFuncTupleIterator {
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final Tuple TUPLE_ONE = tupleFactory.newTuple(Arrays.asList(1));
  private static final Tuple TUPLE_TWO = tupleFactory.newTuple(Arrays.asList(2));
  private static final Tuple TUPLE_THREE = tupleFactory.newTuple(Arrays.asList(3));
  private static final Tuple[] DATA = { TUPLE_ONE, TUPLE_TWO, TUPLE_THREE };

  private final LoadFunc loadFunc = new LoadFunc() {
    Iterator<Tuple> itr = Arrays.asList(DATA).iterator();

    @Override
    public InputFormat<?, ?> getInputFormat() throws IOException {
      return null;
    }

    @Override
    public Tuple getNext() throws IOException {
      if (itr.hasNext())
        return itr.next();
      return null;
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
        throws IOException {
    }

    @Override
    public void setLocation(String arg0, Job arg1) throws IOException {
    }
  };

  @Test
  public void basics() {
    LoadFuncTupleIterator itr = new LoadFuncTupleIterator(loadFunc);
    for (int i = 0; i < DATA.length; ++i) {
      Assert.assertTrue(itr.hasNext());
      Tuple t = itr.next();
      Assert.assertNotNull(t);
      Assert.assertEquals(DATA[i], t);
    }
  }

  @Test
  public void multipleCallsToHasNext() {
    LoadFuncTupleIterator itr = new LoadFuncTupleIterator(loadFunc);
    for (int i = 0; i < DATA.length; ++i) {
      Assert.assertTrue(itr.hasNext());
      Assert.assertTrue(itr.hasNext());
      Tuple t = itr.next();
      Assert.assertNotNull(t);
      Assert.assertEquals(DATA[i], t);
    }
  }

  @Test
  public void nextWithoutHasNext() {
    LoadFuncTupleIterator itr = new LoadFuncTupleIterator(loadFunc);
    for (int i = 0; i < DATA.length; ++i) {
      Tuple t = itr.next();
      Assert.assertNotNull(t);
      Assert.assertEquals(DATA[i], t);
    }
  }
}
