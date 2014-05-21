package com.twitter.elephantbird.util;

import com.twitter.elephantbird.mapreduce.input.combine.CompositeInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestSplitUtil {
  private Configuration conf;

  class DummyInputSplit extends InputSplit {
    private final long length;
    private final String[] locations;

    public DummyInputSplit(long length, String[] locations) {
      this.length = length;
      this.locations = locations;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public String[] getLocations() {
      return locations;
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(SplitUtil.COMBINE_SPLIT_SIZE, 1000);
  }

  @Test
  public void test1() throws IOException, InterruptedException {
    List<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(500, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(400, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(400, new String[] { "l1", "l4", "l5" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(result.size(), 2);
    int index = 0;
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      if (index == 0) {
        Assert.assertEquals(2, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(500, split.getLength(0));
        Assert.assertEquals(400, split.getLength(1));
      } else {
        Assert.assertEquals(1, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l4", "l5" });
        Assert.assertEquals(400, split.getLength(0));
      }
      index++;
    }
  }

  @Test
  public void test2() throws IOException, InterruptedException {
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(600, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(700, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(800, new String[] { "l1", "l4", "l5" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(result.size(), 3);
    int index = 0;
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      if (index == 0) {
        checkLocations(split.getLocations(), new String[] { "l1", "l4", "l5" });
        Assert.assertEquals(1, len);
        Assert.assertEquals(800, split.getLength(0));
      } else if (index == 1) {
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(1, len);
        Assert.assertEquals(700, split.getLength(0));
      } else {
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(1, len);
        Assert.assertEquals(600, split.getLength(0));
      }
      index++;
    }
  }

  @Test
  public void test3() throws IOException, InterruptedException {
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(500, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l4", "l5" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(1, result.size());
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      Assert.assertEquals(3, len);
      checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3", "l4", "l5" });
      Assert.assertEquals(500, split.getLength(0));
      Assert.assertEquals(200, split.getLength(1));
      Assert.assertEquals(100, split.getLength(2));
    }
  }

  @Test
  public void test4() throws IOException, InterruptedException {
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(500, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l4", "l5" }));
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(500, new String[] { "l1", "l4", "l5" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(2, result.size());
    int idx = 0;
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      if (idx == 0) {
        Assert.assertEquals(2, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l4", "l5" });
        Assert.assertEquals(500, split.getLength(0));
        Assert.assertEquals(100, split.getLength(1));
      } else {
        Assert.assertEquals(4, len);
        Assert.assertEquals(500, split.getLength(0));
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(200, split.getLength(1));
        Assert.assertEquals(200, split.getLength(2));
        Assert.assertEquals(100, split.getLength(3));
      }
      idx++;
    }
  }

  @Test
  public void test5() throws IOException, InterruptedException {
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(600, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(500, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(400, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(300, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l2", "l3" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(3, result.size());
    int idx = 0;
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      if (idx == 0) {
        Assert.assertEquals(2, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(600, split.getLength(0));
        Assert.assertEquals(400, split.getLength(1));
      } else if (idx == 1) {
        Assert.assertEquals(3, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(500, split.getLength(0));
        Assert.assertEquals(300, split.getLength(1));
        Assert.assertEquals(200, split.getLength(2));
      } else {
        Assert.assertEquals(1, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(100, split.getLength(0));
      }
      idx++;
    }
  }

  @Test
  public void test6() throws IOException, InterruptedException {
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(300, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(400, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(500, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(600, new String[] { "l1", "l2", "l3" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(3, result.size());
    int idx = 0;
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      if (idx == 0) {
        Assert.assertEquals(2, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(600, split.getLength(0));
        Assert.assertEquals(400, split.getLength(1));
      } else if (idx == 1) {
        Assert.assertEquals(3, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(500, split.getLength(0));
        Assert.assertEquals(300, split.getLength(1));
        Assert.assertEquals(200, split.getLength(2));
      } else {
        Assert.assertEquals(1, len);
        checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3" });
        Assert.assertEquals(100, split.getLength(0));
      }
      idx++;
    }
  }

  @Test
  public void test7() throws IOException, InterruptedException {
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l1", "l4", "l5" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(result.size(), 1);
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      Assert.assertEquals(3, len);
      checkLocations(split.getLocations(), new String[] { "l1", "l2", "l3", "l4", "l5" });
      Assert.assertEquals(200, split.getLength(0));
      Assert.assertEquals(100, split.getLength(1));
      Assert.assertEquals(100, split.getLength(2));
    }
  }

  @Test
  public void test8() throws IOException, InterruptedException {
    // verify locations in order
    ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
    rawSplits.add(new DummyInputSplit(100, new String[] { "l1", "l2", "l3" }));
    rawSplits.add(new DummyInputSplit(200, new String[] { "l3", "l4", "l5" }));
    rawSplits.add(new DummyInputSplit(400, new String[] { "l5", "l6", "l1" }));
    List<CompositeInputSplit> result = SplitUtil.getCombinedCompositeSplits(rawSplits, conf);
    Assert.assertEquals(result.size(), 1);
    for (CompositeInputSplit split : result) {
      int len = split.getSplits().size();
      Assert.assertEquals(3, len);
      checkLocationOrdering(split.getLocations(), new String[] { "l5", "l3", "l1", "l2", "l4" });
      Assert.assertEquals(400, split.getLength(0));
      Assert.assertEquals(200, split.getLength(1));
      Assert.assertEquals(100, split.getLength(2));
    }
  }

  private void checkLocations(String[] actual, String[] expected) {
    Set<String> expectedSet = new HashSet<String>();
    Collections.addAll(expectedSet, expected);
    int count = 0;
    for (String str : actual) {
      if (expectedSet.contains(str)) {
        count++;
      }
    }
    Assert.assertEquals(count, expected.length);
  }

  private void checkLocationOrdering(String[] actual, String[] expected) {
    actual = Arrays.copyOf(actual, actual.length);
    Arrays.sort(actual);
    expected = Arrays.copyOf(expected, expected.length);
    Arrays.sort(expected);
    Assert.assertEquals(expected.length, actual.length);
    for (int i = 0; i < actual.length; i++) {
      Assert.assertEquals(expected[i], actual[i]);
    }
  }
}
