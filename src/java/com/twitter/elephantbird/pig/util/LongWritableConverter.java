package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.LongWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion between Pig types and {@link LongWritable}.
 *
 * @author Andy Schlaikjer
 */
public class LongWritableConverter extends AbstractWritableConverter<LongWritable> {
  public LongWritableConverter() {
    super();
    this.writable = new LongWritable();
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.LONG);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToLong(dataByteArray.get());
  }

  @Override
  protected String toCharArray(LongWritable writable) throws IOException {
    return String.valueOf(writable.get());
  }

  @Override
  protected Integer toInteger(LongWritable writable) throws IOException {
    return (int) writable.get();
  }

  @Override
  protected Long toLong(LongWritable writable) throws IOException {
    return writable.get();
  }

  @Override
  protected Float toFloat(LongWritable writable) throws IOException {
    return (float) writable.get();
  }

  @Override
  protected Double toDouble(LongWritable writable) throws IOException {
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
  protected LongWritable toWritable(String value, boolean newInstance) throws IOException {
    Preconditions.checkNotNull(value);
    return toWritable(Long.parseLong(value), newInstance);
  }

  @Override
  protected LongWritable toWritable(Integer value, boolean newInstance) throws IOException {
    Preconditions.checkNotNull(value);
    return toWritable(value.longValue(), newInstance);
  }

  @Override
  protected LongWritable toWritable(Long value, boolean newInstance) throws IOException {
    Preconditions.checkNotNull(value);
    if (newInstance) {
      writable = new LongWritable();
    }
    writable.set(value);
    return writable;
  }

  @Override
  protected LongWritable toWritable(Float value, boolean newInstance) throws IOException {
    Preconditions.checkNotNull(value);
    return toWritable(value.longValue(), newInstance);
  }

  @Override
  protected LongWritable toWritable(Double value, boolean newInstance) throws IOException {
    Preconditions.checkNotNull(value);
    return toWritable(value.longValue(), newInstance);
  }
}
