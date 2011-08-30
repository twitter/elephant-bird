package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.commons.cli.ParseException;
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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;

import com.twitter.elephantbird.mapreduce.input.RawSequenceFileInputFormat;
import com.twitter.elephantbird.pig.util.TextConverter;
import com.twitter.elephantbird.pig.util.WritableConverter;

/**
 * Pig LoadFunc supporting conversion from key, value objects stored within {@link SequenceFile}s to
 * Pig objects. Example:
 *
 * <pre>
 * key_val = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 *   'com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   'com.twitter.elephantbird.pig.util.TextConverter'
 * ) as (
 *   key: int,
 *   val: chararray
 * );
 *
 * -- or, making use of defaults
 * key_val = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader () as (
 *   key: chararray,
 *   val: chararray
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 * @see WritableConverter
 */
public class SequenceFileLoader<K extends Writable, V extends Writable> extends FileInputLoadFunc
    implements LoadPushDown, LoadMetadata {
  private static Properties createProperties(String converterClassName) {
    Preconditions.checkNotNull(converterClassName);
    Properties properties = new Properties();
    properties.setProperty(CONVERTER_PARAM, converterClassName.trim());
    return properties;
  }

  protected static final String CONVERTER_PARAM = "converter";
  protected static final String READ_KEY_PARAM = "_readKey";
  protected static final String READ_VALUE_PARAM = "_readValue";
  private final DataByteArray keyDataByteArray = new DataByteArray();
  private final DataByteArray valueDataByteArray = new DataByteArray();
  protected final WritableConverter<K> keyConverter;
  protected final WritableConverter<V> valueConverter;
  private final List<Object> tuple2 = Arrays.asList(new Object(), new Object()), tuple1 = Arrays
      .asList(new Object()), tuple0 = Collections.emptyList();
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  protected String signature;
  private RecordReader<DataInputBuffer, DataInputBuffer> reader;
  private boolean readKey = true, readValue = true;

  /**
   * @param keyConverterClassName name of WritableConverter implementation to use for keys.
   * @param valueConverterClassName name of WritableConverter implementation to use for values.
   * @throws ClassNotFoundException
   * @throws ClassCastException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public SequenceFileLoader(String keyConverterClassName, String valueConverterClassName)
      throws ClassNotFoundException, ClassCastException, InstantiationException,
      IllegalAccessException {
    this(createProperties(keyConverterClassName), createProperties(valueConverterClassName));
  }

  /**
   * Default constructor. Defaults used for all options.
   */
  public SequenceFileLoader() throws ClassCastException, ParseException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    this(new Properties(), new Properties());
  }

  @SuppressWarnings({ "unchecked" })
  protected SequenceFileLoader(Properties keyProperties, Properties valueProperties)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Preconditions.checkNotNull(keyProperties);
    Preconditions.checkNotNull(valueProperties);
    String keyConverterClassName =
        keyProperties.getProperty(CONVERTER_PARAM, TextConverter.class.getName());
    keyConverter = (WritableConverter<K>) Class.forName(keyConverterClassName).newInstance();
    String valueConverterClassName =
        valueProperties.getProperty(CONVERTER_PARAM, TextConverter.class.getName());
    valueConverter = (WritableConverter<V>) Class.forName(valueConverterClassName).newInstance();
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
    ResourceFieldSchema keySchema = keyConverter.getLoadSchema();
    keySchema.setName("key");
    ResourceFieldSchema valueSchema = valueConverter.getLoadSchema();
    valueSchema.setName("value");
    resourceSchema.setFields(new ResourceFieldSchema[] { keySchema, valueSchema });
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
