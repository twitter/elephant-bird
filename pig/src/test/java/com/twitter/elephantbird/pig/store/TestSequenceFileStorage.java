package com.twitter.elephantbird.pig.store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.elephantbird.mapreduce.input.RawSequenceFileInputFormat;
import com.twitter.elephantbird.mapreduce.input.RawSequenceFileRecordReader;
import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.util.GenericWritableConverter;
import com.twitter.elephantbird.pig.util.IntWritableConverter;
import com.twitter.elephantbird.pig.util.LoadFuncTupleIterator;
import com.twitter.elephantbird.pig.util.NullWritableConverter;
import com.twitter.elephantbird.pig.util.PigTestUtil;
import com.twitter.elephantbird.pig.util.TextConverter;

/**
 * Tests for {@link SequenceFileStorage} and related utilities.
 *
 * @author Andy Schlaikjer
 * @see SequenceFileLoader
 * @see SequenceFileStorage
 * @see RawSequenceFileInputFormat
 * @see RawSequenceFileRecordReader
 * @see IntWritableConverter
 * @see TextWritableConverter
 * @see NullWritableConverter
 */
public class TestSequenceFileStorage {
  private static final String LINE_ONE = "one, two, buckle my shoe";
  private static final String LINE_TWO = "three, four, shut the door";
  private static final String LINE_THREE = "five, six, something else";
  private static final String[] DATA = { LINE_ONE, LINE_TWO, LINE_THREE };
  private static final String[][] EXPECTED = { { "0", LINE_ONE }, { "1", LINE_TWO },
          { "2", LINE_THREE } };

  private PigServer pigServer;
  private String tempFilename;

  @Before
  public void setUp() throws Exception {
    // create local Pig server
    pigServer = PigTestUtil.makePigServer();

    // create temp SequenceFile
    File tempFile = File.createTempFile("test", ".txt");
    tempFilename = tempFile.getAbsolutePath();
    Path path = new Path("file:///" + tempFilename);
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    IntWritable key = new IntWritable();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
      for (int i = 0; i < DATA.length; ++i) {
        key.set(i);
        value.set(DATA[i]);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  private void registerLoadQuery(Class<?> keyConverterClass, String keyConverterCtorArgs,
      Class<?> keyWritableClass, Class<?> valueConverterClass, String valueConverterCtorArgs,
      Class<?> valueWritableClass, String schema) throws IOException {
    pigServer.registerQuery(String.format(
        "A = LOAD 'file:%s' USING %s('%s', '%s') %s;",
        tempFilename,
        SequenceFileLoader.class.getName(),
        buildWritableConverterArgString(keyConverterClass, keyConverterCtorArgs, keyWritableClass),
        buildWritableConverterArgString(valueConverterClass, valueConverterCtorArgs,
            valueWritableClass), schema == null ? "" : " AS (" + schema + ")"));
  }

  private String buildWritableConverterArgString(Class<?> converterClass, String converterCtorArgs,
      Class<?> writableClass) {
    return (converterClass == null ? "" : "-c " + converterClass.getName())
        + (writableClass == null ? "" : " -t " + writableClass.getName())
        + (converterCtorArgs == null ? "" : " " + converterCtorArgs);
  }

  private void registerLoadQuery(Class<?> keyConverterClass, Class<?> valueConverterClass,
      String schema) throws IOException {
    registerLoadQuery(keyConverterClass, null, null, valueConverterClass, null, null, schema);
  }

  private void registerLoadQuery(Class<?> keyConverterClass, String keyConverterCtorArgs)
      throws IOException {
    registerLoadQuery(keyConverterClass, keyConverterCtorArgs, null, TextConverter.class, null,
        null, null);
  }

  private void registerLoadQuery() throws IOException {
    registerLoadQuery(IntWritableConverter.class, TextConverter.class, "key: int, value: chararray");
  }

  @Test
  public void writableConverterArguments01() throws IOException {
    registerLoadQuery(FixedArgsConstructorIntWritableConverter.class, "123 456");
    pigServer.dumpSchema("A");
  }

  @Test(expected = Exception.class)
  public void writableConverterArguments02() throws IOException {
    registerLoadQuery(FixedArgsConstructorIntWritableConverter.class, "");
    pigServer.dumpSchema("A");
  }

  @Test(expected = Exception.class)
  public void writableConverterArguments03() throws IOException {
    registerLoadQuery(FixedArgsConstructorIntWritableConverter.class, "-123 -456");
    pigServer.dumpSchema("A");
  }

  @Test
  public void writableConverterArguments04() throws IOException {
    registerLoadQuery(FixedArgsConstructorIntWritableConverter.class, "-- -123 -456");
    pigServer.dumpSchema("A");
  }

  @Test(expected = Exception.class)
  public void writableConverterArguments05() throws IOException {
    registerLoadQuery(VarArgsConstructorIntWritableConverter.class, "");
    pigServer.dumpSchema("A");
  }

  @Test
  public void writableConverterArguments06() throws IOException {
    registerLoadQuery(VarArgsConstructorIntWritableConverter.class, "1");
    pigServer.dumpSchema("A");
  }

  @Test
  public void writableConverterArguments07() throws IOException {
    registerLoadQuery(VarArgsConstructorIntWritableConverter.class, "1 2 3 4 5");
    pigServer.dumpSchema("A");
  }

  @Test
  public void readOutsidePig() throws ClassCastException, ParseException, ClassNotFoundException,
      InstantiationException, IllegalAccessException, IOException, InterruptedException {
    // simulate Pig front-end runtime
    final SequenceFileLoader<IntWritable, Text> storage =
        new SequenceFileLoader<IntWritable, Text>("-c " + IntWritableConverter.class.getName(),
            "-c " + TextConverter.class.getName());
    Job job = new Job();
    storage.setUDFContextSignature("12345");
    storage.setLocation(tempFilename, job);

    // simulate Pig back-end runtime
    RecordReader<DataInputBuffer, DataInputBuffer> reader = new RawSequenceFileRecordReader();
    FileSplit fileSplit =
        new FileSplit(new Path(tempFilename), 0, new File(tempFilename).length(),
            new String[] { "localhost" });
    TaskAttemptContext context =
        HadoopCompat.newTaskAttemptContext(HadoopCompat.getConfiguration(job), new TaskAttemptID());
    reader.initialize(fileSplit, context);
    InputSplit[] wrappedSplits = new InputSplit[] { fileSplit };
    int inputIndex = 0;
    List<OperatorKey> targetOps = Arrays.asList(new OperatorKey("54321", 0));
    int splitIndex = 0;
    PigSplit split = new PigSplit(wrappedSplits, inputIndex, targetOps, splitIndex);
    split.setConf(HadoopCompat.getConfiguration(job));
    storage.prepareToRead(reader, split);

    // read tuples and validate
    validate(new LoadFuncTupleIterator(storage));
  }

  @Test
  public void read() throws IOException {
    registerLoadQuery();
    validate(pigServer.openIterator("A"));
  }

  @Test(expected = Exception.class)
  public void readWithMissingWritableConverterArguments() throws IOException {
    registerLoadQuery(FixedArgsConstructorIntWritableConverter.class, TextConverter.class,
        "key: int, value: chararray");
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void readWithoutSchemaTestSchema() throws IOException {
    registerLoadQuery(IntWritableConverter.class, TextConverter.class, null);
    Schema schema = pigServer.dumpSchema("A");
    Assert.assertNotNull(schema);
    Assert.assertEquals("key", schema.getField(0).alias);
    Assert.assertEquals(DataType.INTEGER, schema.getField(0).type);
    Assert.assertEquals("value", schema.getField(1).alias);
    Assert.assertEquals(DataType.CHARARRAY, schema.getField(1).type);
  }

  @Test(expected = FrontendException.class)
  public void readWithBadSchema() throws IOException {
    registerLoadQuery(IntWritableConverter.class, TextConverter.class,
        "key: int, value: chararray, bad: int");
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void readPushKeyProjection() throws IOException {
    registerLoadQuery();
    pigServer.registerQuery("B = FOREACH A GENERATE key;");
    validateIndex(pigServer.openIterator("B"), 1, 0, 0);
  }

  @Test
  public void readPushValueProjection() throws IOException {
    registerLoadQuery();
    pigServer.registerQuery("B = FOREACH A GENERATE value;");
    validateIndex(pigServer.openIterator("B"), 1, 0, 1);
  }

  @Test
  public void readWriteRead() throws IOException {
    registerLoadQuery();
    tempFilename = tempFilename + "-2";
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(), IntWritableConverter.class.getName(),
        TextConverter.class.getName()));
    registerLoadQuery();
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void readWriteNullKeysRead() throws IOException {
    registerLoadQuery();
    tempFilename = tempFilename + "-2";
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(), NullWritableConverter.class.getName(),
        TextConverter.class.getName()));
    registerLoadQuery(NullWritableConverter.class, TextConverter.class, null);
    validateIndex(pigServer.openIterator("A"), 2, 1, 1);
  }

  @Test
  public void readWriteNullValuesRead() throws IOException {
    registerLoadQuery();
    tempFilename = tempFilename + "-2";
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(), IntWritableConverter.class.getName(),
        NullWritableConverter.class.getName()));
    registerLoadQuery(IntWritableConverter.class, NullWritableConverter.class, null);
    validateIndex(pigServer.openIterator("A"), 2, 0, 0);
  }

  @Test
  public void readWriteUnexpectedNullValuesRead() throws IOException {
    registerLoadQuery();
    tempFilename = tempFilename + "-2";
    // swap last value with null; this pair should not be stored
    pigServer.registerQuery(String
        .format("A = FOREACH A GENERATE key, (key == 2 ? null : value) AS value;"));
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(), IntWritableConverter.class.getName(),
        TextConverter.class.getName()));
    registerLoadQuery();
    // validation against expected pairs will succeed, with expected number of pairs one less than
    // usual (the last pair wasn't stored due to null value)
    validate(pigServer.openIterator("A"), DATA.length - 1);
  }

  @Test
  public void readByteArraysWriteByteArraysRead() throws IOException {
    registerLoadQuery(GenericWritableConverter.class, GenericWritableConverter.class,
        "key:bytearray, value:bytearray");
    tempFilename = tempFilename + "-2";
    pigServer
        .registerQuery(String.format(
            "STORE A INTO 'file:%s' USING %s('-c %s -t %s', '-c %s -t %s');", tempFilename,
            SequenceFileStorage.class.getName(), GenericWritableConverter.class.getName(),
            IntWritable.class.getName(), GenericWritableConverter.class.getName(),
            Text.class.getName()));
    registerLoadQuery();
    validate(pigServer.openIterator("A"));
  }

  @Test(expected = Exception.class)
  public void readByteArraysWriteByteArraysWithoutTypeRead() throws IOException {
    registerLoadQuery(GenericWritableConverter.class, TextConverter.class,
        "key:bytearray, value:bytearray");
    tempFilename = tempFilename + "-2";
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(),
        GenericWritableConverter.class.getName(), TextConverter.class.getName()));
    registerLoadQuery();
    validate(pigServer.openIterator("A"));
  }

  @Test(expected = IOException.class)
  public void writeUnsupportedConversion() throws IOException {
    registerLoadQuery();
    // swap ordering of key and value
    pigServer.registerQuery("A = FOREACH A GENERATE TOTUPLE(key), value;");
    // the following should die because IntWritableConverter doesn't support conversion of Tuple to
    // IntWritable
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s-2' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(), IntWritableConverter.class.getName(),
        TextConverter.class.getName()));
  }

  @Test
  public void writeTextConversion() throws IOException {
    registerLoadQuery();
    tempFilename = tempFilename + "-2";
    // rely on TextConverter for conversion of int to Text
    pigServer.registerQuery(String.format("STORE A INTO 'file:%s' USING %s('-c %s', '-c %s');",
        tempFilename, SequenceFileStorage.class.getName(), TextConverter.class.getName(),
        TextConverter.class.getName()));
    registerLoadQuery(TextConverter.class, TextConverter.class, "key:chararray, value:chararray");
    validate(pigServer.openIterator("A"));
  }

  protected void validate(Iterator<Tuple> it, int expectedTupleCount) throws ExecException {
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = it.next();
      Assert.assertNotNull(tuple);
      Assert.assertEquals(2, tuple.size());
      for (int i = 0; i < 2; ++i) {
        Object entry = tuple.get(i);
        Assert.assertNotNull(entry);
        Assert.assertEquals(EXPECTED[tupleCount][i], entry.toString());
      }
      tupleCount++;
    }
    Assert.assertEquals(expectedTupleCount, tupleCount);
  }

  protected void validate(Iterator<Tuple> it) throws ExecException {
    validate(it, EXPECTED.length);
  }

  protected void validateIndex(Iterator<Tuple> it, int expectedTupleSize, int testTupleIndex,
      int expectedTupleIndex) throws ExecException {
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = it.next();
      Assert.assertNotNull(tuple);
      Assert.assertEquals(expectedTupleSize, tuple.size());
      Object entry = tuple.get(testTupleIndex);
      Assert.assertNotNull(entry);
      Assert.assertEquals(EXPECTED[tupleCount][expectedTupleIndex], entry.toString());
      tupleCount++;
    }
    Assert.assertEquals(EXPECTED.length, tupleCount);
  }
}
