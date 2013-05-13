package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Preconditions;
import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.util.GenericWritableConverter;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import com.twitter.elephantbird.pig.util.SequenceFileConfig;
import com.twitter.elephantbird.pig.util.WritableConverter;

/**
 * Pig StoreFunc supporting conversion from Pig tuples to arbitrary key-value pairs stored within
 * {@link SequenceFile}s. Example usage:
 *
 * <pre>
 * pairs = LOAD '$INPUT' AS (key: int, value: chararray);
 *
 * STORE pairs INTO '$OUTPUT' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
 *   '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-c com.twitter.elephantbird.pig.util.TextConverter'
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 * @see SequenceFileConfig
 * @see WritableConverter
 * @see SequenceFileLoader
 */
public class SequenceFileStorage<K extends Writable, V extends Writable> extends BaseStoreFunc {
  /**
   * Failure modes for use with {@link PigCounterHelper} to keep track of runtime error counts.
   */
  public static enum Error {
    /**
     * Null tuple was supplied to {@link SequenceFileStorage#putNext(Tuple)}.
     */
    NULL_TUPLE,
    /**
     * Tuple supplied to {@link SequenceFileStorage#putNext(Tuple)} whose length is not 2.
     */
    TUPLE_SIZE,
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

  /**
   * Refinement of {@link SequenceFileConfig} which adds key/value "type" option.
   */
  private class Config extends SequenceFileConfig<K, V> {
    public static final String TYPE_PARAM = "type";
    public Class<K> keyClass;
    public Class<V> valueClass;

    public Config(String keyArgs, String valueArgs, String otherArgs)
        throws ParseException, IOException {
      super(keyArgs, valueArgs, otherArgs);
    }

    @Override
    protected Options getKeyValueOptions() {
      @SuppressWarnings("static-access")
      Option typeOption =
          OptionBuilder
              .withLongOpt(TYPE_PARAM)
              .hasArg()
              .withArgName("cls")
              .withDescription(
                  "Writable type of data. Defaults to type returned by getWritableClass()"
                      + " method of configured WritableConverter.").create("t");
      return super.getKeyValueOptions().addOption(typeOption);
    }

    @Override
    protected void initialize() throws IOException {
      /*
       * Attempt to initialize key, value classes using arguments. If user doesn't specify '--type'
       * arg, then class will be null.
       */
      keyClass = getWritableClass(keyArguments.getOptionValue(TYPE_PARAM));
      valueClass = getWritableClass(valueArguments.getOptionValue(TYPE_PARAM));

      // initialize key, value converters
      keyConverter.initialize(keyClass);
      valueConverter.initialize(valueClass);

      // allow converters to define writable classes if not already defined
      if (keyClass == null) {
        keyClass = keyConverter.getWritableClass();
      }
      if (valueClass == null) {
        valueClass = valueConverter.getWritableClass();
      }
    }

    /**
     * @param writableClassName
     * @return {@code null} if writableClassName is {@code null}, otherwise the Class instance named
     * by writableClassName.
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private <W extends Writable> Class<W> getWritableClass(String writableClassName)
        throws IOException {
      if (writableClassName == null) {
        return null;
      }
      try {
        return PigContext.resolveClassName(writableClassName);
      } catch (Exception e) {
        throw new IOException(
            String.format("Failed to load Writable class '%s'", writableClassName),
            e);
      }
    }
  }

  private final Config config;

  /**
   * Parses key and value options from argument strings. Available options for both key and value
   * argument strings match those supported by
   * {@link SequenceFileLoader#SequenceFileLoader(String, String)}, as well as:
   * <dl>
   * <dt>-t|--type cls</dt>
   * <dd>{@link Writable} implementation class of data. If Writable class reported by
   * {@link WritableConverter#getWritableClass()} is null (e.g. when using
   * {@link GenericWritableConverter}), this option must be specified.</dd>
   * </dl>
   *
   * @param keyArgs
   * @param valueArgs
   * @param otherArgs
   * @throws ParseException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public SequenceFileStorage(String keyArgs, String valueArgs, String otherArgs)
      throws ParseException, IOException,
      ClassNotFoundException {
    super();
    this.config = new Config(keyArgs, valueArgs, otherArgs);
  }

  /**
   * Delegates to {@link #SequenceFileStorage(String, String, String)}, passing empty string for
   * {@code otherArgs} param.
   *
   * @param keyArgs
   * @param valueArgs
   * @throws ParseException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public SequenceFileStorage(String keyArgs, String valueArgs) throws ParseException, IOException,
      ClassNotFoundException {
    super();
    this.config = new Config(keyArgs, valueArgs, "");
  }

  /**
   * Default constructor which uses default options for key and value.
   *
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws ParseException
   * @see #SequenceFileStorage(String, String)
   */
  public SequenceFileStorage() throws ParseException, IOException, ClassNotFoundException {
    this("", "", "");
  }

  @Override
  public void checkSchema(ResourceSchema schema) throws IOException {
    Preconditions.checkNotNull(schema, "Schema is null");
    ResourceFieldSchema[] fields = schema.getFields();
    Preconditions.checkNotNull(fields, "Schema fields are undefined");
    Preconditions.checkArgument(2 == fields.length,
        "Expecting 2 schema fields but found %s", fields.length);
    config.keyConverter.checkStoreSchema(fields[0]);
    config.valueConverter.checkStoreSchema(fields[1]);
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path cwd) throws IOException {
    return LoadFunc.getAbsolutePath(location, cwd);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    Configuration conf = HadoopCompat.getConfiguration(job);
    ensureUDFContext(conf);
    verifyWritableClass(config.keyClass, true, config.keyConverter);
    verifyWritableClass(config.valueClass, false, config.valueConverter);
    job.setOutputKeyClass(config.keyClass);
    job.setOutputValueClass(config.valueClass);
    super.setStoreLocation(location, job);
    if ("true".equals(conf.get("output.compression.enabled"))) {
      FileOutputFormat.setCompressOutput(job, true);
      String codec = conf.get("output.compression.codec");
      FileOutputFormat.setOutputCompressorClass(job,
          PigContext.resolveClassName(codec).asSubclass(CompressionCodec.class));
    } else {
      // This makes it so that storing to a directory ending with ".gz" or ".bz2" works.
      setCompression(new Path(location), job);
    }
  }

  private void ensureUDFContext(Configuration conf) throws IOException {
    if (UDFContext.getUDFContext().isUDFConfEmpty()
        && conf.get("pig.udf.context") != null) {
      MapRedUtil.setupUDFContext(conf);
    }
  }

  /**
   * Tests validity of Writable class, ensures consistent error message for both key and value
   * tests.
   *
   * @param writableClass class being tested.
   * @param isKeyClass {@code true} if testing keyClass, {@code false} otherwise.
   * @param writableConverter associated WritableConverter instance.
   */
  private static <W extends Writable> void verifyWritableClass(Class<W> writableClass,
      boolean isKeyClass, WritableConverter<W> writableConverter) {
    Preconditions.checkNotNull(writableClass, "%s Writable class is undefined;"
        + " WritableConverter of type '%s' does not define default Writable type,"
        + " and no type was specified by user", isKeyClass ? "Key" : "Value", writableConverter
        .getClass().getName());
  }

  /**
   * @param path
   * @param job
   */
  private void setCompression(Path path, Job job) {
    CompressionCodecFactory codecFactory =
        new CompressionCodecFactory(HadoopCompat.getConfiguration(job));
    CompressionCodec codec = codecFactory.getCodec(path);
    if (codec != null) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, codec.getClass());
    } else {
      FileOutputFormat.setCompressOutput(job, false);
    }
  }

  @Override
  public OutputFormat<K, V> getOutputFormat() {
    return new SequenceFileOutputFormat<K, V>();
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    // validate input tuple
    if (t == null) {
      incrCounter(Error.NULL_TUPLE, 1);
      return;
    }
    if (t.size() != 2) {
      incrCounter(Error.TUPLE_SIZE, 1);
      return;
    }

    // convert key from pig to writable
    K key = config.keyConverter.toWritable(t.get(0));
    if (key == null) {
      incrCounter(Error.NULL_KEY, 1);
      return;
    }

    // convert value from pig to writable
    V value = config.valueConverter.toWritable(t.get(1));
    if (value == null) {
      incrCounter(Error.NULL_VALUE, 1);
      return;
    }

    // write key-value pair
    writeRecord(key, value);
  }
}
