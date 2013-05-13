package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.CoreTestUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test the LzoTextInputFormat, make sure it splits the file properly and
 * returns the right data.
 */
public class TestLzoTextInputFormat {
  private static final Log LOG = LogFactory.getLog(TestLzoTextInputFormat.class);

  private MessageDigest md5_;
  private final String lzoFileName_ = "part-r-00001" + new LzopCodec().getDefaultExtension();
  private Path outputDir_;

  // Test both bigger outputs and small one chunk ones.
  private static final int OUTPUT_BIG = 10485760;
  private static final int OUTPUT_SMALL = 50000;

  @Before
  public void setUp() throws Exception {
    md5_ = MessageDigest.getInstance("MD5");
    Path testBuildData = new Path(System.getProperty("test.build.data", "data"));
    outputDir_ = new Path(testBuildData, "outputDir");
  }

  /**
   * Make sure the lzo index class works as described.
   */
  @Test
  public void testLzoIndex() {
    LzoIndex index = new LzoIndex();
    assertTrue(index.isEmpty());
    index = new LzoIndex(4);
    index.set(0, 0);
    index.set(1, 5);
    index.set(2, 10);
    index.set(3, 15);
    assertFalse(index.isEmpty());

    assertEquals(0, index.findNextPosition(-1));
    assertEquals(5, index.findNextPosition(1));
    assertEquals(5, index.findNextPosition(5));
    assertEquals(15, index.findNextPosition(11));
    assertEquals(15, index.findNextPosition(15));
    assertEquals(-1, index.findNextPosition(16));

    assertEquals(5, index.alignSliceStartToIndex(3, 20));
    assertEquals(15, index.alignSliceStartToIndex(15, 20));
    assertEquals(10, index.alignSliceEndToIndex(8, 30));
    assertEquals(10, index.alignSliceEndToIndex(10, 30));
    assertEquals(30, index.alignSliceEndToIndex(17, 30));
    assertEquals(LzoIndex.NOT_FOUND, index.alignSliceStartToIndex(16, 20));
  }

  /**
   * Index the file and make sure it splits properly.
   *
   * @throws NoSuchAlgorithmException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testWithIndex() throws NoSuchAlgorithmException, IOException,
      InterruptedException {

    runTest(true, OUTPUT_BIG);
    runTest(true, OUTPUT_SMALL);
  }

  /**
   * Don't index the file and make sure it can be processed anyway.
   *
   * @throws NoSuchAlgorithmException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testWithoutIndex() throws NoSuchAlgorithmException, IOException,
      InterruptedException {

    runTest(false, OUTPUT_BIG);
    runTest(false, OUTPUT_SMALL);
  }

  /**
   * Generate random data, compress it, index and md5 hash the data.
   * Then read it all back and md5 that too, to verify that it all went ok.
   *
   * @param testWithIndex Should we index or not?
   * @param charsToOutput How many characters of random data should we output.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws InterruptedException
   */
  private void runTest(boolean testWithIndex, int charsToOutput) throws IOException,
      NoSuchAlgorithmException, InterruptedException {

    Configuration conf = new Configuration();
    conf.setLong("fs.local.block.size", charsToOutput / 2);
    // reducing block size to force a split of the tiny file
    conf.set("io.compression.codecs", LzopCodec.class.getName());

    Assume.assumeTrue(CoreTestUtil.okToRunLzoTests(conf));

    FileSystem.getLocal(conf).close(); // remove cached filesystem (if any)
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(outputDir_, true);
    localFs.mkdirs(outputDir_);

    Job job = new Job(conf);
    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
    TextOutputFormat.setOutputPath(job, outputDir_);

    TaskAttemptContext attemptContext =
        HadoopCompat.newTaskAttemptContext(HadoopCompat.getConfiguration(job),
            new TaskAttemptID(TaskID.forName("task_201305011733_0001_r_000001"), 2));

    // create some input data
    byte[] expectedMd5 = createTestInput(outputDir_, localFs, attemptContext, charsToOutput);

    if (testWithIndex) {
      Path lzoFile = new Path(outputDir_, lzoFileName_);
      LzoIndex.createIndex(localFs, lzoFile);
    }

    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
    TextInputFormat.setInputPaths(job, outputDir_);

    List<InputSplit> is = inputFormat.getSplits(job);
    //verify we have the right number of lzo chunks
    if (testWithIndex && OUTPUT_BIG == charsToOutput) {
      assertEquals(3, is.size());
    } else {
      assertEquals(1, is.size());
    }

    // let's read it all and calculate the md5 hash
    for (InputSplit inputSplit : is) {
      RecordReader<LongWritable, Text> rr = inputFormat.createRecordReader(
          inputSplit, attemptContext);
      rr.initialize(inputSplit, attemptContext);

      while (rr.nextKeyValue()) {
        Text value = rr.getCurrentValue();

        md5_.update(value.getBytes(), 0, value.getLength());
      }

      rr.close();
    }

    localFs.close();
    assertTrue(Arrays.equals(expectedMd5, md5_.digest()));
  }

  /**
   * Creates an lzo file with random data.
   *
   * @param outputDir Output directory.
   * @param fs File system we're using.
   * @param attemptContext Task attempt context, contains task id etc.
   * @throws IOException
   * @throws InterruptedException
   */
  private byte[] createTestInput(Path outputDir, FileSystem fs, TaskAttemptContext attemptContext,
      int charsToOutput) throws IOException, InterruptedException {

    TextOutputFormat<Text, Text> output = new TextOutputFormat<Text, Text>();
    RecordWriter<Text, Text> rw = null;

    md5_.reset();

    try {
      rw = output.getRecordWriter(attemptContext);

      char[] chars = "abcdefghijklmnopqrstuvwxyz\u00E5\u00E4\u00F6"
          .toCharArray();

      Random r = new Random(System.currentTimeMillis());
      Text key = new Text();
      Text value = new Text();
      int charsMax = chars.length - 1;
      for (int i = 0; i < charsToOutput;) {
        i += fillText(chars, r, charsMax, key);
        i += fillText(chars, r, charsMax, value);
        rw.write(key, value);
        md5_.update(key.getBytes(), 0, key.getLength());
        // text output format writes tab between the key and value
        md5_.update("\t".getBytes("UTF-8"));
        md5_.update(value.getBytes(), 0, value.getLength());
      }
    } finally {
      if (rw != null) {
        rw.close(attemptContext);
        OutputCommitter committer = output.getOutputCommitter(attemptContext);
        committer.commitTask(attemptContext);
        committer.commitJob(attemptContext);
      }
    }

    byte[] result = md5_.digest();
    md5_.reset();
    return result;
  }

  private int fillText(char[] chars, Random r, int charsMax, Text text) {
    StringBuilder sb = new StringBuilder();
    // get a reasonable string length
    int stringLength = r.nextInt(charsMax * 2);
    for (int j = 0; j < stringLength; j++) {
      sb.append(chars[r.nextInt(charsMax)]);
    }
    text.set(sb.toString());
    return stringLength;
  }

}
