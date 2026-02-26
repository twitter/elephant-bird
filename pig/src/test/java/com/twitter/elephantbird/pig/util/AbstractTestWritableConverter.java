package com.twitter.elephantbird.pig.util;

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.elephantbird.mapreduce.input.RawSequenceFileRecordReader;
import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.store.SequenceFileStorage;

/**
 * Base class which facilitates creation of unit tests for {@link WritableConverter}
 * implementations.
 *
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestWritableConverter<W extends Writable, C extends WritableConverter<W>> {
  private final Class<? extends W> writableClass;
  private final Class<? extends C> writableConverterClass;
  private final String writableConverterArguments;
  private final W[] data;
  private final String[] expected;
  private final String valueSchema;
  protected PigServer pigServer;
  protected String tempFilename;

  public AbstractTestWritableConverter(final Class<? extends W> writableClass,
      final Class<? extends C> writableConverterClass, final String writableConverterArguments,
      final W[] data, final String[] expected, final String valueSchema) {
    this.writableClass = writableClass;
    this.writableConverterClass = writableConverterClass;
    this.writableConverterArguments =
        writableConverterArguments == null ? "" : writableConverterArguments;
    this.data = data;
    this.expected = expected;
    this.valueSchema = valueSchema;
  }

  protected void registerReadQuery(String filename, String writableConverterArguments,
      String valueSchema)
      throws IOException {
    pigServer.registerQuery(String.format("A = LOAD 'file:%s' USING %s('-c %s', '-c %s %s')%s;",
        filename, SequenceFileLoader.class.getName(), IntWritableConverter.class.getName(),
        writableConverterClass.getName(), writableConverterArguments, valueSchema == null
            || valueSchema.isEmpty() ? "" : String.format(" AS (key: int, value: %s)", valueSchema)));
  }

  protected void registerReadQuery(String writableConverterArguments,
      String valueSchema) throws IOException {
    registerReadQuery(tempFilename, writableConverterArguments, valueSchema);
  }

  protected void registerReadQuery(String filename) throws IOException {
    registerReadQuery(filename, writableConverterArguments, valueSchema);
  }

  protected void registerReadQuery() throws IOException {
    registerReadQuery(tempFilename, writableConverterArguments, valueSchema);
  }

  protected void registerWriteQuery(String filename, String writableConverterArguments)
      throws IOException {
    pigServer.registerQuery(String.format(
        "STORE A INTO 'file:%s' USING %s('-c %s', '-c %s -t %s -- %s');", filename,
        SequenceFileStorage.class.getName(), IntWritableConverter.class.getName(),
        writableConverterClass.getName(), writableClass.getName(), writableConverterArguments));
  }

  protected void registerWriteQuery(String filename) throws IOException {
    registerWriteQuery(filename, writableConverterArguments);
  }

  @Before
  public void setup() throws IOException {
    // create local Pig server
    pigServer = PigTestUtil.makePigServer();

    // create temp SequenceFile
    final File tempFile = File.createTempFile("test", ".txt");
    tempFilename = tempFile.getAbsolutePath();
    final Path path = new Path("file:///" + tempFilename);
    final Configuration conf = new Configuration();
    final FileSystem fs = path.getFileSystem(conf);
    final IntWritable key = new IntWritable();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), writableClass);
      for (int i = 0; i < data.length; ++i) {
        key.set(i);
        writer.append(key, data[i]);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  @Test
  public void readOutsidePig() throws ClassCastException, ParseException, ClassNotFoundException,
      InstantiationException, IllegalAccessException, IOException, InterruptedException {
    // simulate Pig front-end runtime
    final SequenceFileLoader<IntWritable, Text> loader =
        new SequenceFileLoader<IntWritable, Text>(String.format("-c %s",
            IntWritableConverter.class.getName()), String.format(
            "-c %s %s", writableConverterClass.getName(),
            writableConverterArguments));
    Job job = new Job();
    loader.setUDFContextSignature("12345");
    loader.setLocation(tempFilename, job);

    // simulate Pig back-end runtime
    final RecordReader<DataInputBuffer, DataInputBuffer> reader = new RawSequenceFileRecordReader();
    final FileSplit fileSplit =
        new FileSplit(new Path(tempFilename), 0, new File(tempFilename).length(),
            new String[] { "localhost" });
    final TaskAttemptContext context =
        HadoopCompat.newTaskAttemptContext(HadoopCompat.getConfiguration(job), new TaskAttemptID());
    reader.initialize(fileSplit, context);
    final InputSplit[] wrappedSplits = new InputSplit[] { fileSplit };
    final int inputIndex = 0;
    final List<OperatorKey> targetOps = Arrays.asList(new OperatorKey("54321", 0));
    final int splitIndex = 0;
    final PigSplit split = new PigSplit(wrappedSplits, inputIndex, targetOps, splitIndex);
    split.setConf(HadoopCompat.getConfiguration(job));
    loader.prepareToRead(reader, split);

    // read tuples and validate
    validate(new LoadFuncTupleIterator(loader));
  }

  @Test
  public void read() throws IOException {
    registerReadQuery();
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void readWriteRead() throws IOException {
    registerReadQuery();
    registerWriteQuery(tempFilename + "-2");
    registerReadQuery(tempFilename + "-2");
    validate(pigServer.openIterator("A"));
  }

  protected void validate(String[] expected, Iterator<Tuple> it) throws ExecException {
    int tupleCount = 0;
    for (; it.hasNext(); ++tupleCount) {
      final Tuple tuple = it.next();
      Assert.assertNotNull(tuple);
      Assert.assertEquals(2, tuple.size());
      Object value = tuple.get(1);
      Assert.assertNotNull(value);
      Assert.assertEquals(expected[tupleCount], value.toString());
    }
    Assert.assertEquals(data.length, tupleCount);
  }

  protected void validate(Iterator<Tuple> it) throws ExecException {
    validate(expected, it);
  }
}
