package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;

import com.twitter.elephantbird.mapreduce.input.RawSequenceFileInputFormat;
import com.twitter.elephantbird.pig.store.SequenceFileStorage;
import com.twitter.elephantbird.pig.util.NullWritableConverter;
import com.twitter.elephantbird.pig.util.TextConverter;
import com.twitter.elephantbird.pig.util.WritableConverter;

/**
 * Pig LoadFunc supporting conversion from key, value objects stored within {@link SequenceFile}s to
 * Pig objects. Example usage:
 *
 * <pre>
 * pairs = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 *   '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-c com.twitter.elephantbird.pig.util.TextConverter'
 * ) as (
 *   key: int,
 *   value: chararray
 * );
 *
 * -- or, making use of defaults
 * pairs = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader ();
 * </pre>
 *
 * If the configured {@link WritableConverter} implementation returns type {@link DataType#NULL}
 * from {@link WritableConverter#getLoadSchema()} (e.g. {@link NullWritableConverter}), then the
 * associated values will not be included within loaded tuples. For instance:
 *
 * <pre>
 * values = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 *   '-c com.twitter.elephantbird.pig.util.NullWritableConverter',
 *   '-c com.twitter.elephantbird.pig.util.TextConverter'
 * )
 * DESCRIBE values; -- {(value: chararray)}
 *
 * keys = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 *   '-c com.twitter.elephantbird.pig.util.TextConverter',
 *   '-c com.twitter.elephantbird.pig.util.NullWritableConverter'
 * )
 * DESCRIBE keys; -- {(key: chararray)}
 * </pre>
 *
 * @author Andy Schlaikjer
 * @see WritableConverter
 */
public class SequenceFileLoader<K extends Writable, V extends Writable> extends FileInputLoadFunc
    implements LoadPushDown, LoadMetadata {
  protected static final String CONVERTER_PARAM = "converter";
  protected static final String READ_KEY_PARAM = "_readKey";
  protected static final String READ_VALUE_PARAM = "_readValue";
  protected final CommandLine keyArguments;
  protected final CommandLine valueArguments;
  protected final WritableConverter<K> keyConverter;
  protected final WritableConverter<V> valueConverter;
  private final DataByteArray keyDataByteArray = new DataByteArray();
  private final DataByteArray valueDataByteArray = new DataByteArray();
  private final List<Object> tuple2 = Arrays.asList(new Object(), new Object()), tuple1 = Arrays
      .asList(new Object()), tuple0 = Collections.emptyList();
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  protected String signature;
  private RecordReader<DataInputBuffer, DataInputBuffer> reader;
  private boolean readKey = true, readValue = true;

  /**
   * Parses key and value options from argument strings. Available options for both key and value
   * argument strings include:
   * <dl>
   * <dt>-c|--converter cls</dt>
   * <dd>{@link WritableConverter} implementation class to use for conversion of data. Defaults to
   * {@link TextConverter} for both key and value.</dd>
   * </dl>
   * Any extra arguments found will be treated as String arguments for the WritableConverter
   * constructor. For instance, the argument string {@code "-c MyConverter 123 abc"} specifies
   * WritableConverter class {@code MyConverter} along with two constructor arguments {@code "123"}
   * and {@code "abc"}. This will cause SequenceFileLoader to invoke
   * {@code MyConverter(String arg1, String arg2)} with the given values when creating a new
   * instance of MyConverter. If no such constructor exists, constructor
   * {@code new MyConverter(String[] args)} is attempted. If no such constructor exists, constructor
   * {@code new MyConverter(String argsJoinedOnSpace)} is attempted. If this also fails, a
   * RuntimeException will be thrown.
   *
   * <p>
   * Note that WritableConverter constructor arguments prefixed by one or more hyphens will be
   * interpreted as options for SequenceFileLoader itself, resulting in an
   * {@link UnrecognizedOptionException}. To avoid this, place these values after a {@code --}
   * (double-hyphen) token:
   *
   * <pre>
   * A = LOAD '$data' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
   *   '-c ...IntWritableConverter',
   *   '-c ...MyComplexWritableConverter basic options here -- --complex -options here'
   * );
   * </pre>
   *
   * The above Pig script will cause SequenceFileLoader to attempt execution of
   * {@code MyComplexWritableConverter} constructors in the following order:
   * <ol>
   * <li>
   * <code>MyComplexWritableConverter("basic", "options", "here", "--complex", "-options", "here")</code>
   * </li>
   * <li>
   * <code>MyComplexWritableConverter(new String[]{"basic", "options", "here", "--complex", "-options", "here"})</code>
   * </li>
   * <li><code>MyComplexWritableConverter("basic options here --complex -options here")</code></li>
   * </ol>
   *
   * @param keyArgs
   * @param valueArgs
   * @throws ParseException
   */
  public SequenceFileLoader(String keyArgs, String valueArgs) throws ParseException {
    keyArguments = parseArguments(keyArgs);
    valueArguments = parseArguments(valueArgs);
    keyConverter = getWritableConverter(keyArguments);
    valueConverter = getWritableConverter(valueArguments);
  }

  /**
   * Default constructor. Defaults used for all options.
   */
  public SequenceFileLoader() throws ClassCastException, ParseException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    this("", "");
  }

  /**
   * @return Options instance containing valid key/value options.
   */
  protected Options getOptions() {
    @SuppressWarnings("static-access")
    Option converterOption =
        OptionBuilder
            .withLongOpt(CONVERTER_PARAM)
            .hasArg()
            .withArgName("cls")
            .withDescription(
                "Converter type to use for conversion of data." + "  Defaults to '"
                    + TextConverter.class.getName() + "' for key and value.").create("c");
    return new Options().addOption(converterOption);
  }

  /**
   * @param args
   * @return CommandLine instance containing options parsed from argument string.
   * @throws ParseException
   */
  private CommandLine parseArguments(String args) throws ParseException {
    Options options = getOptions();
    CommandLine cmdline = null;
    try {
      cmdline = new GnuParser().parse(options, args.split(" "));
    } catch (ParseException e) {
      new HelpFormatter().printHelp(SequenceFileStorage.class.getName() + "(keyArgs, valueArgs)",
          options);
      throw e;
    }
    return cmdline;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Writable> WritableConverter<T> getWritableConverter(
      CommandLine arguments) {
    // get remaining non-empty argument strings from commandline
    String[] converterArgs = removeEmptyArgs(arguments.getArgs());
    try {

      // get writable converter class
      Class<WritableConverter<T>> converterClass =
          (Class<WritableConverter<T>>) Class.forName(arguments.getOptionValue(CONVERTER_PARAM,
              TextConverter.class.getName()));

      if (converterArgs == null || converterArgs.length == 0) {

        // use default ctor
        return converterClass.newInstance();

      } else {
        try {

          // look up ctor having explicit number of String arguments
          Class<?>[] parameterTypes = new Class<?>[converterArgs.length];
          Arrays.fill(parameterTypes, String.class);
          Constructor<WritableConverter<T>> ctor = converterClass.getConstructor(parameterTypes);
          return ctor.newInstance((Object[]) converterArgs);

        } catch (NoSuchMethodException e) {
          try {

            // look up ctor having single String[] (or String... varargs) argument
            Constructor<WritableConverter<T>> ctor =
                converterClass.getConstructor(new Class<?>[] { String[].class });
            return ctor.newInstance((Object) converterArgs);

          } catch (NoSuchMethodException e2) {

            // look up ctor having single String argument and join args together
            Constructor<WritableConverter<T>> ctor =
                converterClass.getConstructor(new Class<?>[] { String.class });
            StringBuilder sb = new StringBuilder(converterArgs[0]);
            for (int i = 1; i < converterArgs.length; ++i) {
              sb.append(" ").append(converterArgs[i]);
            }
            return ctor.newInstance(sb.toString());

          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create WritableConverter instance", e);
    }
  }

  private static String[] removeEmptyArgs(String[] args) {
    List<String> converterArgsFiltered = Lists.newArrayList();
    for (String arg : args) {
      if (arg == null || arg.isEmpty())
        continue;
      converterArgsFiltered.add(arg);
    }
    return converterArgsFiltered.toArray(new String[0]);
  }

  private Properties getContextProperties() {
    Preconditions.checkNotNull(signature, "Signature is null");
    return UDFContext.getUDFContext().getUDFProperties(getClass(), new String[] { signature });
  }

  private void setContextProperty(String name, String value) {
    Preconditions.checkNotNull(name, "Context property name is null");
    getContextProperties().setProperty(signature + name, value);
  }

  private String getContextProperty(String name, String defaultValue) {
    return getContextProperties().getProperty(signature + name, defaultValue);
  }

  @Override
  public InputFormat<DataInputBuffer, DataInputBuffer> getInputFormat() throws IOException {
    return new RawSequenceFileInputFormat();
  }

  @Override
  public LoadCaster getLoadCaster() throws IOException {
    /*
     * We have two LoadCasters--one for the key type, another for the value type. Unfortunately,
     * LoadCaster doesn't allow clients to specify which field it's casting (nor schema of field),
     * so we're out of luck here. No casting supported.
     */
    return null;
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.signature = signature;
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return ImmutableList.of(OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
      throws FrontendException {
    readKey = readValue = false;
    for (RequiredField field : requiredFieldList.getFields()) {
      // TODO fix Pig's handling of RequiredField type initialization
      int i = field.getIndex();
      switch (i) {
        case 0:
          readKey = true;
          // try {
          // keyConverter.checkLoadSchema(ResourceSchemaUtil.createResourceFieldSchema(field));
          // } catch (IOException e) {
          // throw new FrontendException("Key schema check failed", e);
          // }
          break;
        case 1:
          readValue = true;
          // try {
          // valueConverter.checkLoadSchema(ResourceSchemaUtil.createResourceFieldSchema(field));
          // } catch (IOException e) {
          // throw new FrontendException("Value schema check failed", e);
          // }
          break;
        default:
          // TODO fix Pig's silent ignorance of FrontendExceptions thrown from here
          throw new FrontendException("Expected field indices in [0, 1] but found index " + i);
      }
    }
    setContextProperty(READ_KEY_PARAM, Boolean.toString(readKey));
    setContextProperty(READ_VALUE_PARAM, Boolean.toString(readValue));
    return new RequiredFieldResponse(true);
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    Preconditions.checkNotNull(location, "Location is null");
    Preconditions.checkNotNull(location, "Job is null");
    FileInputFormat.setInputPaths(job, new Path(location));
    readKey = Boolean.parseBoolean(getContextProperty(READ_KEY_PARAM, "true"));
    readValue = Boolean.parseBoolean(getContextProperty(READ_VALUE_PARAM, "true"));
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    ResourceSchema resourceSchema = new ResourceSchema();
    List<ResourceFieldSchema> fieldSchemas = Lists.newArrayList();

    // determine key field schema
    ResourceFieldSchema keySchema = keyConverter.getLoadSchema();
    if (keySchema == null) {
      keySchema = new ResourceFieldSchema();
      keySchema.setType(DataType.BYTEARRAY);
    }
    keySchema.setName("key");
    if (keySchema.getType() != DataType.NULL) {
      fieldSchemas.add(keySchema);
    }

    // determine value field schema
    ResourceFieldSchema valueSchema = valueConverter.getLoadSchema();
    if (valueSchema == null) {
      valueSchema = new ResourceFieldSchema();
      valueSchema.setType(DataType.BYTEARRAY);
    }
    valueSchema.setName("value");
    if (valueSchema.getType() != DataType.NULL) {
      fieldSchemas.add(valueSchema);
    }

    // return tuple schema
    resourceSchema.setFields(fieldSchemas.toArray(new ResourceFieldSchema[0]));
    return resourceSchema;
  }

  /**
   * This implementation returns {@code null}.
   *
   * @see org.apache.pig.LoadMetadata#getStatistics(java.lang.String,
   *      org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    return null;
  }

  /**
   * This implementation returns {@code null}.
   *
   * @see org.apache.pig.LoadMetadata#getPartitionKeys(java.lang.String,
   *      org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return null;
  }

  /**
   * This implementation throws {@link UnsupportedOperationException}.
   *
   * @see org.apache.pig.LoadMetadata#setPartitionFilter(org.apache.pig.Expression)
   * @throws UnsupportedOperationException
   */
  @Override
  public void setPartitionFilter(Expression expression) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    this.reader = reader;
    keyConverter.initialize(null);
    valueConverter.initialize(null);
    ResourceFieldSchema fieldSchema = keyConverter.getLoadSchema();
    if (fieldSchema != null && fieldSchema.getType() == DataType.NULL) {
      readKey = false;
    }
    fieldSchema = valueConverter.getLoadSchema();
    if (fieldSchema != null && fieldSchema.getType() == DataType.NULL) {
      readValue = false;
    }
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!reader.nextKeyValue())
        return null;
      List<Object> tuple = tuple0;
      if (readKey) {
        if (readValue) {
          tuple = tuple2;
          tuple.set(0, getCurrentKeyObject());
          tuple.set(1, getCurrentValueObject());
        } else {
          tuple = tuple1;
          tuple.set(0, getCurrentKeyObject());
        }
      } else if (readValue) {
        tuple = tuple1;
        tuple.set(0, getCurrentValueObject());
      }
      return tupleFactory.newTupleNoCopy(tuple);
    } catch (InterruptedException e) {
      throw new ExecException("Error while reading input", 6018, PigException.REMOTE_ENVIRONMENT, e);
    }
  }

  private Object getCurrentKeyObject() throws IOException, InterruptedException {
    DataInputBuffer ibuf = reader.getCurrentKey();
    keyDataByteArray.set(Arrays.copyOf(ibuf.getData(), ibuf.getLength()));
    return keyConverter.bytesToObject(keyDataByteArray);
  }

  private Object getCurrentValueObject() throws IOException, InterruptedException {
    DataInputBuffer ibuf = reader.getCurrentValue();
    valueDataByteArray.set(Arrays.copyOf(ibuf.getData(), ibuf.getLength()));
    return valueConverter.bytesToObject(valueDataByteArray);
  }
}
