package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion between Pig types and {@link IntWritable}.
 *
 * @author Andy Schlaikjer
 */
public class IntWritableConverter extends AbstractWritableConverter<IntWritable> {
  public IntWritableConverter() {
    super(new IntWritable());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.INTEGER);
    return schema;
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
  protected IntWritable toWritable(String value) throws IOException {
    return toWritable(Integer.parseInt(value));
  }

  @Override
  protected IntWritable toWritable(Integer value) throws IOException {
    writable.set(value);
    return writable;
  }

  @Override
  protected IntWritable toWritable(Long value) throws IOException {
    return toWritable(value.intValue());
  }

  @Override
  protected IntWritable toWritable(Float value) throws IOException {
    return toWritable(value.intValue());
  }

  @Override
  protected IntWritable toWritable(Double value) throws IOException {
    return toWritable(value.intValue());
  }
}
