package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Delegates to an internal WritableConverter. The WritableConverter implementation used is
 * determined by the runtime type of the data encountered. Below is a table of the WritableConverter
 * implementations used for supported Pig and {@link Writable} data types:
 *
 * <table>
 * <tr>
 * <th>Pig type</th>
 * <th>Writable type</th>
 * <th>WritableConverter type</th>
 * </tr>
 * <tr>
 * <td>{@code int}</td>
 * <td>{@link IntWritable}</td>
 * <td>{@link IntWritableConverter}</td>
 * </tr>
 * <tr>
 * <td>{@code long}</td>
 * <td>{@link LongWritable}</td>
 * <td>{@link LongWritableConverter}</td>
 * </tr>
 * <tr>
 * <td>{@code chararray}</td>
 * <td>{@link Text}</td>
 * <td>{@link TextConverter}</td>
 * </tr>
 * <tr>
 * <td>{@code null}</td>
 * <td>{@link NullWritable}</td>
 * <td>{@link NullWritableConverter}</td>
 * </tr>
 * </table>
 *
 * @author Andy Schlaikjer
 */
public class DefaultWritableConverter<W extends Writable> extends AbstractWritableConverter<W> {
  private static final Map<Byte, Class<? extends WritableConverter<?>>> defaultWritableConverterTypeForPigType =
      Maps.newHashMap();
  private static final Map<Class<? extends Writable>, Class<? extends WritableConverter<?>>> defaultWritableConverterTypeForWritableType =
      Maps.newHashMap();

  /**
   * Registers a default {@link WritableConverter} type to use for the given Pig and
   * {@link Writable} types.
   *
   * @param pigType
   * @param writableClass
   * @param writableConverterClass
   */
  public static <T extends Writable> void registerWritableConverterType(byte pigType,
      Class<T> writableClass, Class<? extends WritableConverter<T>> writableConverterClass) {
    defaultWritableConverterTypeForPigType.put(pigType, writableConverterClass);
    defaultWritableConverterTypeForWritableType.put(writableClass, writableConverterClass);
  }

  static {
    registerWritableConverterType(DataType.INTEGER, IntWritable.class, IntWritableConverter.class);
    registerWritableConverterType(DataType.LONG, LongWritable.class, LongWritableConverter.class);
    registerWritableConverterType(DataType.CHARARRAY, Text.class, TextConverter.class);
    registerWritableConverterType(DataType.NULL, NullWritable.class, NullWritableConverter.class);
  }

  private WritableConverter<W> delegate;

  /**
   * Constructs a suitable WritableConverter for the given Writable type. If the given Writable type
   * is {@code null} or WritableConverter delegate has already been initialized, then nothing is
   * done.
   *
   * @throws IllegalArgumentException if no WritableConverter type is found which supports the given
   *           Writable type.
   */
  @Override
  public void initialize(Class<? extends W> writableClass) {
    // return if argument is undefined or delegate is already initialized
    if (writableClass == null || delegate != null) {
      return;
    }

    // find supporting WritableConverter type
    Class<? extends WritableConverter<?>> writableConverterClass =
        defaultWritableConverterTypeForWritableType.get(writableClass);
    Preconditions.checkArgument(writableConverterClass != null,
        "No WritableConverter type found for Writable type '%s'", writableClass.getName());

    createDelegate(writableConverterClass);
    delegate.initialize(writableClass);
  }

  /**
   * @return {@code null} until {@link #checkStoreSchema(ResourceFieldSchema)} has been called.
   *         Otherwise, delegates to an internal WritableConverter.
   */
  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    if (delegate == null) {
      return null;
    }
    return delegate.getLoadSchema();
  }

  /**
   * Finds a suitable WritableConverter type for the given schema and constructs an instance of it.
   *
   * @throws IllegalArgumentException if no WritableConverter type is found which supports the given
   *           schema.
   */
  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    Preconditions.checkNotNull(schema, "Schema is null");
    Preconditions.checkState(delegate == null, "Delegate has already been initialized");

    // find supporting WritableConverter type
    Class<? extends WritableConverter<?>> writableConverterClass =
        defaultWritableConverterTypeForPigType.get(schema.getType());
    Preconditions.checkArgument(writableConverterClass != null,
        "No WritableConverter type found for schema type '%s'",
        DataType.findTypeName(schema.getType()));

    createDelegate(writableConverterClass);
    delegate.initialize(null);
  }

  /**
   * @return {@code null} until {@link #checkStoreSchema(ResourceFieldSchema)} has been called.
   *         Otherwise, delegates to an internal WritableConverter.
   */
  @Override
  public Class<W> getWritableClass() throws IOException {
    if (delegate == null) {
      return null;
    }
    return delegate.getWritableClass();
  }

  @SuppressWarnings("unchecked")
  private void createDelegate(Class<? extends WritableConverter<?>> writableConverterClass) {
    try {
      delegate = (WritableConverter<W>) writableConverterClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Failed to create WritableConverter instance of type '%s'", writableClass.getName()), e);
    }
  }

  @Override
  public DataBag bytesToBag(byte[] arg0, ResourceFieldSchema arg1) throws IOException {
    return delegate.bytesToBag(arg0, arg1);
  }

  @Override
  public String bytesToCharArray(byte[] arg0) throws IOException {
    return delegate.bytesToCharArray(arg0);
  }

  @Override
  public Double bytesToDouble(byte[] arg0) throws IOException {
    return delegate.bytesToDouble(arg0);
  }

  @Override
  public Float bytesToFloat(byte[] arg0) throws IOException {
    return delegate.bytesToFloat(arg0);
  }

  @Override
  public Integer bytesToInteger(byte[] arg0) throws IOException {
    return delegate.bytesToInteger(arg0);
  }

  @Override
  public Long bytesToLong(byte[] arg0) throws IOException {
    return delegate.bytesToLong(arg0);
  }

  @Override
  public Map<String, Object> bytesToMap(byte[] arg0) throws IOException {
    return delegate.bytesToMap(arg0);
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return delegate.bytesToObject(dataByteArray);
  }

  @Override
  public Tuple bytesToTuple(byte[] arg0, ResourceFieldSchema arg1) throws IOException {
    return delegate.bytesToTuple(arg0, arg1);
  }

  @Override
  public byte[] toBytes(DataBag arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(DataByteArray arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(Double arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(Float arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(Integer arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(Long arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(Map<String, Object> arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(String arg0) throws IOException {
    return delegate.toBytes(arg0);
  }

  @Override
  public byte[] toBytes(Tuple arg0) throws IOException {
    return delegate.toBytes(arg0);
  }
}
