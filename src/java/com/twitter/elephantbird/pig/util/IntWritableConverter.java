package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.IntWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion between Pig int type (Integer) and {@link IntWritable}.
 *
 * @author Andy Schlaikjer
 */
public class IntWritableConverter extends AbstractWritableConverter<IntWritable> {
  public IntWritableConverter() {
    super();
    this.writable = new IntWritable();
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToInteger(dataByteArray.get());
  }

  @Override
  protected String toCharArray(IntWritable writable) throws IOException {
    return String.valueOf(writable.get());
  }

  @Override
  protected Integer toInteger(IntWritable writable) throws IOException {
    return writable.get();
  }

  @Override
  protected Long toLong(IntWritable writable) throws IOException {
    return (long) writable.get();
  }

  @Override
  protected Float toFloat(IntWritable writable) throws IOException {
    return (float) writable.get();
  }

  @Override
  protected Double toDouble(IntWritable writable) throws IOException {
    return (double) writable.get();
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    switch (schema.getType()) {
      case DataType.CHARARRAY:
      case DataType.INTEGER:
      case DataType.LONG:
      case DataType.FLOAT:
      case DataType.DOUBLE:
        return;
    }
    throw new IOException("Pig type '" + DataType.findTypeName(schema.getType()) + "' unsupported");
  }

  @Override
  protected IntWritable toWritable(String value, boolean newInstance) throws IOException {
    return toWritable(Integer.parseInt(value), newInstance);
  }

  @Override
  protected IntWritable toWritable(Integer value, boolean newInstance) throws IOException {
    Preconditions.checkNotNull(value);
    if (newInstance)
      return new IntWritable(value);
    if (writable == null)
      writable = new IntWritable();
    writable.set(value);
    return writable;
  }

  @Override
  protected IntWritable toWritable(Long value, boolean newInstance) throws IOException {
    return toWritable(value.intValue(), newInstance);
  }

  @Override
  protected IntWritable toWritable(Float value, boolean newInstance) throws IOException {
    return toWritable(value.intValue(), newInstance);
  }

  @Override
  protected IntWritable toWritable(Double value, boolean newInstance) throws IOException {
    return toWritable(value.intValue(), newInstance);
  }
}
