package com.twitter.elephantbird.pig.load;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.Lists;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.data.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Alex Levenson
 */
public class TestLuceneIndexLoader {

  private static class Loader extends LuceneIndexLoader<NullWritable> {
    public Loader(String[] args) {
      super(args);
    }

    @Override
    protected Tuple recordToTuple(int key, NullWritable value) {
      return null;
    }

    @Override
    protected LuceneIndexInputFormat<NullWritable> getLuceneIndexInputFormat() throws IOException {
      return null;
    }
  }

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testConstructor() {
    try {
      new Loader(new String[]{});
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      new Loader(new String[]{"invalid"});
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      new Loader(new String[]{"invalid", "extra"});
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      new Loader(new String[]{"--queries"});
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      new Loader(new String[]{"--file"});
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      new Loader(new String[]{"--file", "one", "two"});
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    // valid constructor usages
    new Loader(new String[]{"--queries", "query1"});
    new Loader(new String[]{"--queries", "query1", "query2", "query3"});
    new Loader(new String[]{"--file",
        "src/test/resources/com/twitter/elephantbird/pig/load/queryfile.txt"});
  }

  private void doTestSetLocation(Loader loader) throws Exception {
    Job job = createStrictMock(Job.class);
    Configuration conf = new Configuration();
    expect(HadoopCompat.getConfiguration(job)).andStubReturn(conf);
    replay(job);

    loader.setLocation(tempDir.getRoot()
      .getAbsolutePath(), job);
    assertEquals(Lists.newArrayList("+hello -goodbye", "+test", "+こにちは"),
      LuceneIndexInputFormat.getQueries(conf));

    assertEquals(1, LuceneIndexInputFormat.getInputPaths(conf).length);
    assertTrue(LuceneIndexInputFormat.getInputPaths(conf)[0]
      .toString()
      .endsWith(tempDir.getRoot().getAbsolutePath()));
  }

  @Test
  public void testSetLocationQueries() throws Exception {
    doTestSetLocation(new Loader(new String[]{"--queries", "+hello -goodbye", "+test", "+こにちは"}));
  }

  @Test
  public void testSetLocationFileMissing() throws Exception {
    String fakeFile = new File(tempDir.getRoot(), "nonexistant").getAbsolutePath();
    try {
      doTestSetLocation(new Loader(new String[]{"--file", fakeFile}));
      fail("This should throw an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().endsWith("/nonexistant does not exist!"));
    }
  }

  @Test
  public void testSetLocationFile() throws Exception {
    doTestSetLocation(new Loader(new String[]{"--file",
      "src/test/resources/com/twitter/elephantbird/pig/load/queryfile.txt"}));
  }

}