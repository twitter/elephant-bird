package com.twitter.elephantbird.mapreduce.output;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.HadoopCompat;

import com.twitter.elephantbird.util.LzoUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestLzoTextOutputFormat {
  private static final long SMALL_MIN_SIZE = 1000L;
  private static final long BIG_MIN_SIZE = SMALL_MIN_SIZE * 1000;
  private Path outputDir_;
  private Configuration conf_;
  private FileSystem lfs_;

  @Before
  public void setUp() throws Exception {
    outputDir_ = new Path(System.getProperty("test.build.data", "data"),
        "outputDir");
    conf_ = new Configuration();
    conf_.setBoolean(LzoUtils.LZO_OUTPUT_INDEX, true);
    lfs_ = new RawLocalFileSystem();
    lfs_.initialize(URI.create("file:///"), conf_);
    FileSystem.closeAll(); // purge fs cache
  }

  @After
  public void cleanup() throws Exception {
    lfs_.delete(outputDir_, true);
  }

  private void testIndexFile(long minSize, boolean viaBlockSize)
      throws Exception {
    final Job job = new Job(conf_);
    final Configuration conf = job.getConfiguration();
    if (viaBlockSize) {
      conf.setLong("fs.local.block.size", minSize);
    } else {
      conf.setLong("fs.local.block.size", 1L); // would always index
      conf.setLong(LzoUtils.LZO_OUTPUT_INDEXABLE_MINSIZE, minSize);
    }
    FileOutputFormat.setOutputPath(job, outputDir_);
    final LzoTextOutputFormat<Text,Text> outputFormat =
        new LzoTextOutputFormat<Text,Text>();
    final TaskAttemptContext attemptContext =
        HadoopCompat.newTaskAttemptContext(HadoopCompat.getConfiguration(job),
            new TaskAttemptID(TaskID.forName("task_1234567_0001_r_000001"), 1));
    final RecordWriter writer = outputFormat.getRecordWriter(attemptContext);
    for (int i = 0; i < 1024; i++) {
      writer.write(new Text(UUID.randomUUID().toString()),
          new Text(UUID.randomUUID().toString()));
    }
    writer.close(attemptContext);
    final Path lzoFile = outputFormat.getDefaultWorkFile(attemptContext,
        LzopCodec.DEFAULT_LZO_EXTENSION);
    final Path lzoIndexFile = lzoFile.suffix(LzoIndex.LZO_INDEX_SUFFIX);
    assertTrue(lzoFile + ": Lzo file should exist!", lfs_.exists(lzoFile));

    if (minSize == SMALL_MIN_SIZE) {
      assertTrue(lzoIndexFile + ": Lzo index file should exist!",
          lfs_.exists(lzoIndexFile));
    } else {
      assertFalse(lzoIndexFile + ": Lzo index file should not exist!",
          lfs_.exists(lzoIndexFile));
    }
  }

  @Test
  public void testLzoIndexViaBlockSize() throws Exception {
    testIndexFile(SMALL_MIN_SIZE, true);
  }

  @Test
  public void testNoLzoIndexViaBlockSize() throws Exception {
    testIndexFile(BIG_MIN_SIZE, true);
  }

  @Test
  public void testLzoIndexViaMinSize() throws Exception {
    testIndexFile(SMALL_MIN_SIZE, false);
  }

  @Test
  public void testNoLzoIndexViaMinSize() throws Exception {
    testIndexFile(BIG_MIN_SIZE, false);
  }
}
