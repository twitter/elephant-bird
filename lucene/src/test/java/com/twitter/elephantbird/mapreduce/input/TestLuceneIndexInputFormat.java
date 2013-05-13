package com.twitter.elephantbird.mapreduce.input;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat.LuceneIndexInputSplit;
import com.twitter.elephantbird.mapreduce.output.LuceneIndexOutputFormat;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Levenson
 */
public class TestLuceneIndexInputFormat {

  @Test
  public void testFindSplitsRecursive() throws Exception {
    findSplitsHelper(ImmutableList.of(new Path("src/test/resources/com/twitter/elephantbird"
      + "/mapreduce/input/sample_indexes/")));
  }

  @Test
  public void testFindSplitsExplicit() throws Exception {
    findSplitsHelper(ImmutableList.of(
      new Path("src/test/resources/com/twitter/elephantbird/mapreduce/input/"
        + "sample_indexes/index-1"),
      new Path("src/test/resources/com/twitter/elephantbird/mapreduce/input/"
        + "sample_indexes/index-2"),
      new Path("src/test/resources/com/twitter/elephantbird/mapreduce/input/"
        + "sample_indexes/more-indexes/index-3")));
  }

  private static class DummyLuceneInputFormat extends LuceneIndexInputFormat<IntWritable> {
    @Override
    public PathFilter getIndexDirPathFilter(Configuration conf) throws IOException {
      return LuceneIndexOutputFormat.newIndexDirFilter(conf);
    }

    @Override
    public RecordReader<IntWritable, IntWritable>
    createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      return null;
    }
  }

  private void findSplitsHelper(List<Path> inputPaths) throws IOException {
    DummyLuceneInputFormat lif = new DummyLuceneInputFormat();

    Configuration conf = new Configuration();

    LuceneIndexInputFormat.setInputPaths(inputPaths, conf);

    lif.loadConfig(conf);
    PriorityQueue<LuceneIndexInputSplit> splits = lif.findSplits(conf);
    LuceneIndexInputSplit split;
    split = splits.poll();
    assertEquals(4, split.getLength());
    assertTrue(split.getIndexDirs().get(0).toString().endsWith("sample_indexes/index-1"));

    split = splits.poll();
    assertEquals(6, split.getLength());
    assertTrue(split.getIndexDirs().get(0).toString().endsWith("sample_indexes/more-indexes/index-3"));

    split = splits.poll();
    assertEquals(20, split.getLength());
    assertTrue(split.getIndexDirs().get(0).toString().endsWith("sample_indexes/index-2"));

    assertTrue(splits.isEmpty());
  }

  @Test
  public void testGetSplits() throws Exception {
    DummyLuceneInputFormat lif = new DummyLuceneInputFormat();

    Configuration conf = new Configuration();

    LuceneIndexInputFormat.setInputPaths(
      ImmutableList.of(new Path("src/test/resources/com/twitter/elephantbird"
      + "/mapreduce/input/sample_indexes/")), conf);
    LuceneIndexInputFormat.setMaxCombinedIndexSizePerSplitBytes(15L, conf);
    JobContext jobContext = createStrictMock(JobContext.class);
    expect(HadoopCompat.getConfiguration(jobContext)).andStubReturn(conf);
    replay(jobContext);
    List<InputSplit> splits = lif.getSplits(jobContext);
    LuceneIndexInputSplit split = (LuceneIndexInputSplit) splits.get(0);
    assertEquals(2, split.getIndexDirs().size());
    assertTrue(split.getIndexDirs().get(0).toString().endsWith("sample_indexes/index-1"));
    assertTrue(split.getIndexDirs().get(1).toString()
        .endsWith("sample_indexes/more-indexes/index-3"));
    split = (LuceneIndexInputSplit) splits.get(1);
    assertEquals(1, split.getIndexDirs().size());
    assertTrue(split.getIndexDirs().get(0).toString().endsWith("sample_indexes/index-2"));
  }

  @Test
  public void testLuceneIndexInputSplit() throws Exception {
    LuceneIndexInputSplit orig = new LuceneIndexInputSplit(
        Lists.newArrayList(new Path("/index/test"),
                           new Path("/index/test2"),
                           new Path("/index/test3")), 500L);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);

    orig.write(dataOut);

    LuceneIndexInputSplit deSerialized = new LuceneIndexInputSplit();
    deSerialized.readFields(new DataInputStream((new ByteArrayInputStream(bytesOut.toByteArray()))));

    assertEquals(orig.getIndexDirs(), deSerialized.getIndexDirs());
    assertEquals(orig.getLength(), deSerialized.getLength());

    assertEquals(0, orig.compareTo(deSerialized));

    LuceneIndexInputSplit smaller = new LuceneIndexInputSplit(
        Lists.newArrayList(new Path("/index/small")), 100L);

    assertTrue(orig.compareTo(smaller) > 0);
    assertTrue(smaller.compareTo(orig) < 0);
  }

  @Test
  public void testCombineSplits() throws Exception {
    DummyLuceneInputFormat lif = new DummyLuceneInputFormat();

    PriorityQueue<LuceneIndexInputSplit> splits = new PriorityQueue<LuceneIndexInputSplit>();
    String[] paths = new String[] {
        "/index/1", "/index/2", "/index/3", "/index/4", "/index/5", "/index/6"};
    Long[] sizes = new Long[]{500L, 300L, 100L, 150L, 1200L, 500L};

    for (int i = 0; i < paths.length; i++) {
      splits.add(new LuceneIndexInputSplit(Lists.newArrayList(new Path(paths[i])), sizes[i]));
    }

    List<InputSplit> combined = lif.combineSplits(splits, 1000L, 10000L);
    assertEquals(3, combined.size());

    List<Path> dirs = ((LuceneIndexInputSplit) combined.get(0)).getIndexDirs();
    Set<String> dirsStrings = Sets.newHashSet(Iterables.transform(dirs,
        Functions.toStringFunction()));
    assertEquals(3, dirsStrings.size());
    assertTrue(dirsStrings.contains("/index/2"));
    assertTrue(dirsStrings.contains("/index/3"));
    assertTrue(dirsStrings.contains("/index/4"));

    dirs = ((LuceneIndexInputSplit) combined.get(1)).getIndexDirs();
    dirsStrings = Sets.newHashSet(Iterables.transform(dirs, Functions.toStringFunction()));
    assertEquals(2, dirsStrings.size());
    assertTrue(dirsStrings.contains("/index/1"));
    assertTrue(dirsStrings.contains("/index/6"));

    dirs = ((LuceneIndexInputSplit) combined.get(2)).getIndexDirs();
    dirsStrings = Sets.newHashSet(Iterables.transform(dirs, Functions.toStringFunction()));
    assertEquals(1, dirsStrings.size());
    assertTrue(dirsStrings.contains("/index/5"));
  }

  @Test
  public void testCombineSplitsOneSplit() throws Exception {
    DummyLuceneInputFormat lif = new DummyLuceneInputFormat();

    PriorityQueue<LuceneIndexInputSplit> splits = new PriorityQueue<LuceneIndexInputSplit>();
    splits.add(new LuceneIndexInputSplit(Lists.newArrayList(new Path("/index/1")), 1500L));

    List<InputSplit> combined = lif.combineSplits(splits, 1000L, 10000L);
    assertEquals(1, combined.size());

    List<Path> dirs = ((LuceneIndexInputSplit) combined.get(0)).getIndexDirs();
    Set<String> dirsStrings = Sets.newHashSet(Iterables.transform(dirs,
      Functions.toStringFunction()));
    assertEquals(1, dirsStrings.size());
    assertTrue(dirsStrings.contains("/index/1"));
  }

  @Test
  public void testCombineSplitsAllTooBig() throws Exception {
    DummyLuceneInputFormat lif = new DummyLuceneInputFormat();

    PriorityQueue<LuceneIndexInputSplit> splits = new PriorityQueue<LuceneIndexInputSplit>();
    String[] paths = new String[]{"/index/1", "/index/2", "/index/3"};
    Long[] sizes = new Long[]{1500L, 1501L, 1502L};
    for (int i = 0; i < paths.length; i++) {
      splits.add(new LuceneIndexInputSplit(Lists.newArrayList(new Path(paths[i])), sizes[i]));
    }

    List<InputSplit> combined = lif.combineSplits(splits, 1000L, 10000L);
    assertEquals(3, combined.size());

    for (int i=0; i < paths.length; i++) {
      List<Path> dirs = ((LuceneIndexInputSplit) combined.get(i)).getIndexDirs();
      List<String> dirsStrings = Lists.newLinkedList(Iterables.transform(dirs,
          Functions.toStringFunction()));
      assertEquals(1, dirsStrings.size());
      assertEquals("/index/" + String.valueOf(i + 1), dirsStrings.get(0));
    }
  }

  @Test
  public void testCombineSplitsWithMaxNumberIndexesPerMapper() throws Exception {
    DummyLuceneInputFormat lif = new DummyLuceneInputFormat();

    PriorityQueue<LuceneIndexInputSplit> splits = new PriorityQueue<LuceneIndexInputSplit>();
    String[] paths = new String[1000];
    long[] sizes = new long[1000];

    for (int i = 0; i< 100; i++) {
      switch (i) {
        case 0:
          sizes[i] = 500L;
          paths[i] = "/index/500";
          break;
        case 1:
          sizes[i] = 300L;
          paths[i] = "/index/300";
          break;
        case 2:
          sizes[i] = 100L;
          paths[i] = "/index/100";
          break;
        default:
          sizes[i] = 1L;
          paths[i] = "/index/small-" + i;
      }
      splits.add(new LuceneIndexInputSplit(Lists.newArrayList(new Path(paths[i])), sizes[i]));
    }

    List<InputSplit> combined = lif.combineSplits(splits, 150L, 10L);
    assertEquals(12, combined.size());

    for (int i = 0; i < 9; i++) {
      LuceneIndexInputSplit split = (LuceneIndexInputSplit) combined.get(i);
      assertEquals(10L, split.getIndexDirs().size());
      assertEquals(10L, split.getLength());
      for (Path p : split.getIndexDirs()) {
        assertTrue(p.toString().startsWith("/index/small-"));
      }
    }

    LuceneIndexInputSplit split = (LuceneIndexInputSplit) combined.get(9);
    assertEquals(8, split.getIndexDirs().size());
    assertEquals(107, split.getLength());
    for (int i = 0; i < 7; i++) {
      assertTrue(split.getIndexDirs().get(i).toString().startsWith("/index/small-"));
    }
    assertEquals("/index/100", split.getIndexDirs().get(7).toString());

    split = (LuceneIndexInputSplit) combined.get(10);
    assertEquals(1, split.getIndexDirs().size());
    assertEquals(300, split.getLength());
    assertEquals("/index/300", split.getIndexDirs().get(0).toString());

    split = (LuceneIndexInputSplit) combined.get(11);
    assertEquals(1, split.getIndexDirs().size());
    assertEquals(500, split.getLength());
    assertEquals("/index/500", split.getIndexDirs().get(0).toString());
  }

  @Test
  public void testQuerySerialization() throws Exception {
    Configuration conf = new Configuration();
    List<String> queries =  ImmutableList.of(
      "+one -two",
      "something, with, commas",
      "something 東京 with unicode",
      "\"something with quotes\"");
    LuceneIndexInputFormat.setQueries(queries, conf);
    assertEquals(queries, LuceneIndexInputFormat.getQueries(conf));
  }
}
