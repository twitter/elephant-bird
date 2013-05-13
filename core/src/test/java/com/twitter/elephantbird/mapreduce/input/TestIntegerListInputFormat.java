package com.twitter.elephantbird.mapreduce.input;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Before;
import org.junit.Test;

public class TestIntegerListInputFormat {
  IntegerListInputFormat input_;
  JobContext jc_;

  @Before
  public void setup() {
    input_ = new IntegerListInputFormat();
    jc_ = HadoopCompat.newJobContext(new Configuration(), new JobID());
  }

  @Test
  public void testEvenSplits() throws IOException, InterruptedException {
    IntegerListInputFormat.setListInterval(-6, 84);
    IntegerListInputFormat.setNumSplits(7);

    List<InputSplit> splits = input_.getSplits(jc_);
    assertEquals(splits.size(), 7);

    assertRange(splits.get(0), -6, 6);
    assertRange(splits.get(1), 7, 19);
    assertRange(splits.get(2), 20, 32);
    assertRange(splits.get(3), 33, 45);
    assertRange(splits.get(4), 46, 58);
    assertRange(splits.get(5), 59, 71);
    assertRange(splits.get(6), 72, 84);
  }

  @Test
  public void testUnvenSplits() throws IOException, InterruptedException {
    IntegerListInputFormat.setListInterval(-6, 83);
    IntegerListInputFormat.setNumSplits(7);

    List<InputSplit> splits = input_.getSplits(jc_);
    assertEquals(splits.size(), 7);

    assertRange(splits.get(0), -6, 6);
    assertRange(splits.get(1), 7, 19);
    assertRange(splits.get(2), 20, 32);
    assertRange(splits.get(3), 33, 45);
    assertRange(splits.get(4), 46, 58);
    assertRange(splits.get(5), 59, 71);
    assertRange(splits.get(6), 72, 83);
  }

  @Test
  public void testSmallSplits() throws IOException, InterruptedException {
    IntegerListInputFormat.setListInterval(1, 5);
    IntegerListInputFormat.setNumSplits(6);

    List<InputSplit> splits = input_.getSplits(jc_);
    assertEquals(splits.size(), 5);

    assertRange(splits.get(0), 1, 1);
    assertRange(splits.get(1), 2, 2);
    assertRange(splits.get(2), 3, 3);
    assertRange(splits.get(3), 4, 4);
    assertRange(splits.get(4), 5, 5);
  }

  private void assertRange(InputSplit split, long min, long max) {
    IntegerListInputSplit realSplit = (IntegerListInputSplit)split;
    assertEquals(realSplit.getMin(), min);
    assertEquals(realSplit.getMax(), max);
  }
}
