package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
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
import com.twitter.elephantbird.pig.util.TextConverter;
import com.twitter.elephantbird.pig.util.WritableConverter;

/**
 * Pig StoreFunc supporting conversion between Pig tuples and arbitrary key-value pairs stored
 * within {@link SequenceFile}s. Usage:
 *
 * <pre>
 * key_val = LOAD '$INPUT' AS (f0, key: int, f2, val: chararray);
 *
 * STORE key_val INTO '$OUTPUT' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
 *   '-i 1 -t org.apache.hadoop.io.IntWritable -c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-i 3 -t org.apache.hadoop.io.Text        -c com.twitter.elephantbird.pig.util.TextConverter'
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 */
public class SequenceFileStorage<K extends Writable, V extends Writable> extends
    SequenceFileLoader<K, V> implements StoreFuncInterface {
  protected static Properties parseArgumentString(String args) throws ParseException {
    // define options
    @SuppressWarnings("static-access")
    Option indexOption =
        OptionBuilder
            .withLongOpt(INDEX_PARAM)
            .hasArg()
            .withArgName("i")
            .withDescription(
                "Tuple index from which to read data." + " Defaults to '0' for key, '1' for value.")
            .create("i");
    @SuppressWarnings("static-access")
    Option typeOption =
        OptionBuilder
            .withLongOpt(TYPE_PARAM)
            .hasArg()
            .withArgName("cls")
            .withDescription(
                "Writable type of data." + " Defaults to '" + Text.class.getName()
                    + "' for key and value.").create("t");
    @SuppressWarnings("static-access")
    Option converterOption =
        OptionBuilder
            .withLongOpt(CONVERTER_PARAM)
            .hasArg()
            .withArgName("cls")
            .withDescription(
                "Converter type to use for conversion of data." + "  Defaults to '"
                    + TextConverter.class.getName() + "' for key and value.").create("c");
    Options options = new Options();
    for (Option option : ImmutableList.of(indexOption, typeOption, converterOption))
      options.addOption(option);

    // parse key args and initialize members
    CommandLine cmdline = null;
    try {
      cmdline = new GnuParser().parse(options, args.split(" "));
    } catch (ParseException e) {
      new HelpFormatter().printHelp(SequenceFileStorage.class.getName() + "(keyArgs, valueArgs)",
          options);
      throw e;
    }

    // convert to Properties
    Properties properties = new Properties();
    for (String name : Arrays.asList(INDEX_PARAM, TYPE_PARAM, CONVERTER_PARAM)) {
      String value = cmdline.getOptionValue(name);
      if (value != null) {
        properties.setProperty(name, value);
      }
    }
    return properties;
  }

  protected static final String INDEX_PARAM = "index";
  protected static final String TYPE_PARAM = "type";
  private final int keyIndex, valueIndex, maxIndex;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private RecordWriter<K, V> writer;

  /**
   * Parses key and value options from argument strings. Available options for both key and value
   * argument strings include:
   * <dl>
   * <dt>-i|--index n</dt>
   * <dd>Tuple index from which to read data. Defaults to 0 for key, 1 for value.</dd>
   * <dt>-t|--type cls</dt>
   * <dd>{@link Writable} implementation class of data. Defaults to {@link Text} for both key and
   * value.</dd>
   * <dt>-c|--converter cls</dt>
   * <dd>{@link WritableConverter} implementation class to use for conversion of data. Defaults to
   * {@link TextConverter} for both key and value.</dd>
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
  public SequenceFileStorage(String keyArgs, String valueArgs) throws ParseException,
      ClassNotFoundException, ClassCastException, InstantiationException, IllegalAccessException {
    this(parseArgumentString(keyArgs), parseArgumentString(valueArgs));
  }

  /**
   * Default constructor which uses default options for key and value.
   *
   * @see #SequenceFileStorage(String, String)
   */
  public SequenceFileStorage() throws ClassCastException, ParseException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    this(new Properties(), new Properties());
  }

  @SuppressWarnings("unchecked")
  protected SequenceFileStorage(Properties keyProperties, Properties valueProperties)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    super(keyProperties, valueProperties);
    keyIndex = Integer.parseInt(keyProperties.getProperty(INDEX_PARAM, "0"));
    valueIndex = Integer.parseInt(valueProperties.getProperty(INDEX_PARAM, "1"));
    maxIndex = Math.max(keyIndex, valueIndex);
    keyClass =
        (Class<K>) Class.forName(keyProperties.getProperty(TYPE_PARAM, Text.class.getName()));
    valueClass =
        (Class<V>) Class.forName(valueProperties.getProperty(TYPE_PARAM, Text.class.getName()));
    keyConverter.initialize(keyClass);
    valueConverter.initialize(valueClass);
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
    if (maxIndex >= fields.length)
      throw new IOException("Expecting at least " + (maxIndex + 1)
          + " schema entries but found only " + fields.length);
    keyConverter.checkStoreSchema(fields[keyIndex]);
    valueConverter.checkStoreSchema(fields[valueIndex]);
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
    Preconditions.checkNotNull(t);
    Preconditions.checkArgument(t.size() > maxIndex, "Tuple size %d must be greater than %d",
        t.size(), maxIndex);
    K key = keyConverter.toWritable(t.get(keyIndex));
    V value = valueConverter.toWritable(t.get(valueIndex));
    try {
      writer.write(key, value);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    // copied from PigStorage
    StoreFunc.cleanupOnFailureImpl(location, job);
  }
}
