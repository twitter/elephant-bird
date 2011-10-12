package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;

import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import com.twitter.elephantbird.pig.util.WritableConverter;

/**
 * Pig StoreFunc supporting conversion between Pig tuples and arbitrary key-value pairs stored
 * within {@link SequenceFile}s. Example usage:
 *
 * <pre>
 * pairs = LOAD '$INPUT' AS (key: int, value: chararray);
 *
 * STORE pairs INTO '$OUTPUT' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
 *   '-t org.apache.hadoop.io.IntWritable -c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-t org.apache.hadoop.io.Text        -c com.twitter.elephantbird.pig.util.TextConverter'
 * );
 * </pre>
 *
 * If key or value output type is {@link NullWritable}, no {@link WritableConverter} implementation
 * must be specified; SequenceFileStorage will use {@link NullWritable#get()} directly:
 *
 * <pre>
 * values = LOAD '$INPUT' AS (value: int);
 *
 * STORE values INTO '$OUTPUT' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
 *   '-t org.apache.hadoop.io.NullWritable',
 *   '-t org.apache.hadoop.io.IntWritable -c com.twitter.elephantbird.pig.util.IntWritableConverter'
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 */
public class SequenceFileStorage<K extends Writable, V extends Writable> extends
    SequenceFileLoader<K, V> implements StoreFuncInterface {
  /**
   * Failure modes for use with {@link PigCounterHelper} to keep track of runtime error counts.
   *
   * @author Andy Schlaikjer
   */
  public static enum Error {
    /**
     * Null tuple was supplied to {@link SequenceFileStorage#putNext(Tuple)}.
     */
    NULL_TUPLE,
    /**
     * Null key was supplied to {@link SequenceFileStorage#putNext(Tuple)} and key type is not
     * {@link NullWritable}.
     */
    NULL_KEY,
    /**
     * Null value was supplied to {@link SequenceFileStorage#putNext(Tuple)} and value type is not
     * {@link NullWritable}.
     */
    NULL_VALUE;
  }

  protected static final String TYPE_PARAM = "type";
  private final PigCounterHelper counterHelper = new PigCounterHelper();
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private RecordWriter<K, V> writer;

  /**
   * Parses key and value options from argument strings. Available options for both key and value
   * argument strings include those supported by
   * {@link SequenceFileLoader#SequenceFileLoader(String, String)}, as well as:
   * <dl>
   * <dt>-t|--type cls</dt>
   * <dd>{@link Writable} implementation class of data. Defaults to {@link Text} for both key and
   * value.</dd>
   * </dl>
   *
   * @param keyArgs
   * @param valueArgs
   * @throws ParseException
   * @throws ClassNotFoundException
   * @throws ClassCastException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  @SuppressWarnings("unchecked")
  public SequenceFileStorage(String keyArgs, String valueArgs) throws ParseException,
      ClassNotFoundException, ClassCastException, InstantiationException, IllegalAccessException {
    super(keyArgs, valueArgs);
    keyClass =
        (Class<K>) Class.forName(keyArguments.getOptionValue(TYPE_PARAM, Text.class.getName()));
    valueClass =
        (Class<V>) Class.forName(valueArguments.getOptionValue(TYPE_PARAM, Text.class.getName()));
    Preconditions.checkState(!(keyClass == NullWritable.class && valueClass == NullWritable.class),
        "Both key and value types are '%s'", NullWritable.class);
  }

  /**
   * Default constructor which uses default options for key and value.
   *
   * @see #SequenceFileStorage(String, String)
   */
  public SequenceFileStorage() throws ClassCastException, ParseException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    this("", "");
  }

  @Override
  protected Options getOptions() {
    @SuppressWarnings("static-access")
    Option typeOption =
        OptionBuilder
            .withLongOpt(TYPE_PARAM)
            .hasArg()
            .withArgName("cls")
            .withDescription(
                "Writable type of data." + " Defaults to '" + Text.class.getName()
                    + "' for key and value.").create("t");
    return super.getOptions().addOption(typeOption);
  }

  @Override
  public OutputFormat<K, V> getOutputFormat() throws IOException {
    return new SequenceFileOutputFormat<K, V>();
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    this.signature = signature;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException, ClassCastException {
    job.setOutputKeyClass(keyClass);
    job.setOutputValueClass(valueClass);
    FileOutputFormat.setOutputPath(job, new Path(location));
    if ("true".equals(job.getConfiguration().get("output.compression.enabled"))) {
      FileOutputFormat.setCompressOutput(job, true);
      String codec = job.getConfiguration().get("output.compression.codec");
      try {
        FileOutputFormat.setOutputCompressorClass(job,
            Class.forName(codec).asSubclass(CompressionCodec.class));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Class not found: " + codec);
      }
    } else {
      // This makes it so that storing to a directory ending with ".gz" or ".bz2" works.
      setCompression(new Path(location), job);
    }
  }

  /**
   * @param path
   * @param job
   */
  private void setCompression(Path path, Job job) {
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(job.getConfiguration());
    CompressionCodec codec = codecFactory.getCodec(path);
    if (codec != null) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, codec.getClass());
    } else {
      FileOutputFormat.setCompressOutput(job, false);
    }
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path cwd) throws IOException {
    // copied from PigStorage
    return LoadFunc.getAbsolutePath(location, cwd);
  }

  @Override
  public void checkSchema(ResourceSchema schema) throws IOException {
    Preconditions.checkNotNull(schema, "Schema is null");
    ResourceFieldSchema[] fields = schema.getFields();
    Preconditions.checkNotNull(fields, "Schema fields are undefined");
    int i = 0;
    if (keyClass != NullWritable.class) {
      checkFieldSchema(fields, i++, keyClass, keyConverter);
    }
    if (valueClass != NullWritable.class) {
      checkFieldSchema(fields, i, valueClass, valueConverter);
    }
  }

  private <T extends Writable> void checkFieldSchema(ResourceFieldSchema[] fields, int index,
      Class<T> writableClass, WritableConverter<T> writableConverter) throws IOException {
    Preconditions.checkArgument(fields.length > index,
        "Expecting schema length > %s but found length %s", index, fields.length);
    writableConverter.checkStoreSchema(fields[index]);
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
    keyConverter.initialize(keyClass);
    valueConverter.initialize(valueClass);
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    // test for null input tuple
    if (t == null) {
      counterHelper.incrCounter(Error.NULL_TUPLE, 1);
      return;
    }

    // tuple index
    int i = 0;

    // convert key from pig to writable
    K key = null;
    if (keyClass == NullWritable.class) {
      key = getNullWritable();
    } else {
      Object obj = t.get(i++);
      if (obj == null) {
        counterHelper.incrCounter(Error.NULL_KEY, 1);
        return;
      }
      key = keyConverter.toWritable(obj);
    }

    // convert value from pig to writable
    V value = null;
    if (valueClass == NullWritable.class) {
      value = getNullWritable();
    } else {
      Object obj = t.get(i);
      if (obj == null) {
        counterHelper.incrCounter(Error.NULL_VALUE, 1);
        return;
      }
      value = valueConverter.toWritable(obj);
    }

    // write key-value pair
    try {
      writer.write(key, value);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Writable> T getNullWritable() {
    return (T) NullWritable.get();
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    // copied from PigStorage
    StoreFunc.cleanupOnFailureImpl(location, job);
  }
}
