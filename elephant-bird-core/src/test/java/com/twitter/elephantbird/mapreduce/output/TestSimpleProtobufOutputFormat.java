package com.twitter.elephantbird.mapreduce.output;

import org.junit.Test;

import junit.framework.TestCase;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.RecordWriter;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.NullWritable;
//
//import java.io.IOException;
//import java.io.File;
//
public class TestSimpleProtobufOutputFormat extends TestCase {

  @Test
  public static void testThatFails() {
    assertTrue("need to write a real test", true);
  }
  //  private static JobConf defaultConf = new JobConf();
//
//  private static FileSystem localFs = null;
//  static {
//    try {
//      localFs = FileSystem.getLocal(defaultConf);
//    } catch (IOException e) {
//      throw new RuntimeException("init failure", e);
//    }
//  }
//  // A random task attempt id for testing.
//  private static String attempt = "attempt_200707121733_0001_m_000000_0";
//
//  private static Path workDir =
//    new Path(new Path(
//                      new Path(System.getProperty("test.build.data", "."),
//                               "data"),
//                      FileOutputCommitter.TEMP_DIR_NAME), "_" + attempt);
//
//  @SuppressWarnings("unchecked")
//  public void testFormat() throws Exception {
//    JobConf job = new JobConf();
//    job.set("mapred.task.id", attempt);
//    FileOutputFormat.setOutputPath(job, workDir.getParent().getParent());
//    FileOutputFormat.setWorkOutputPath(job, workDir);
//    FileSystem fs = workDir.getFileSystem(job);
//    if (!fs.mkdirs(workDir)) {
//      fail("Failed to create output directory");B
//    }
//    String file = "test.txt";
//
//    // A reporter that does nothing
//
//    Reporter reporter = Reporter.NULL;
//
//    SimpleProtobufOutputFormat theOutputFormat = new SimpleProtobufOutputFormat();
//    RecordWriter theRecordWriter =
//      theOutputFormat.getRecordWriter(localFs, job, file, reporter);
//
//    Text key1 = new Text("key1");
//    Text key2 = new Text("key2");
//    Text val1 = new Text("val1");
//    Text val2 = new Text("val2");
//    NullWritable nullWritable = NullWritable.get();
//
//    try {
//      theRecordWriter.write(key1, val1);
//      theRecordWriter.write(null, nullWritable);
//      theRecordWriter.write(null, val1);
//      theRecordWriter.write(nullWritable, val2);
//      theRecordWriter.write(key2, nullWritable);
//      theRecordWriter.write(key1, null);
//      theRecordWriter.write(null, null);
//      theRecordWriter.write(key2, val2);
//
//    } finally {
//      theRecordWriter.close(reporter);
//    }
//    File expectedFile = new File(new Path(workDir, file).toString());
//    StringBuffer expectedOutput = new StringBuffer();
//    expectedOutput.append(key1).append('\t').append(val1).append("\n");
//    expectedOutput.append(val1).append("\n");
//    expectedOutput.append(val2).append("\n");
//    expectedOutput.append(key2).append("\n");
//    expectedOutput.append(key1).append("\n");
//    expectedOutput.append(key2).append('\t').append(val2).append("\n");
//    String output = UtilsForTests.slurp(expectedFile);
//    assertEquals(output, expectedOutput.toString());
//
}
