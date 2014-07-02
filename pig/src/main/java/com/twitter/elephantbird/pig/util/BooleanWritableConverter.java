package com.twitter.elephantbird.pig.util;

import com.twitter.elephantbird.pig.util.AbstractWritableConverter;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

import java.io.IOException;

/**
 * Supports conversion between Pig types and {@link org.apache.hadoop.io.BooleanWritable}.
 * 
 * @author Xu Wenhao
 */
public class BooleanWritableConverter extends AbstractWritableConverter<BooleanWritable> {
  public BooleanWritableConverter() {
    super(new BooleanWritable());
  }

  @Override
  public ResourceSchema.ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceSchema.ResourceFieldSchema schema = new ResourceSchema.ResourceFieldSchema();
    schema.setType(DataType.INTEGER);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToInteger(dataByteArray.get());
  }

  @Override
  protected String toCharArray(BooleanWritable writable) throws IOException {
    return String.valueOf(writable.get());
  }

  @Override
  protected Integer toInteger(BooleanWritable writable) throws IOException {
    return writable.get() ? 1 : 0;
  }

  @Override
  protected Long toLong(BooleanWritable writable) throws IOException {
    return (long)( writable.get() ? 1:0);
  }

  @Override
  protected Float toFloat(BooleanWritable writable) throws IOException {
    return (float)( writable.get() ? 1:0);
  }

  @Override
  protected Double toDouble(BooleanWritable writable) throws IOException {
    return (double)( writable.get() ? 1: 0);
  }

  @Override
  public void checkStoreSchema(ResourceSchema.ResourceFieldSchema schema) throws IOException {
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
  protected BooleanWritable toWritable(String value) throws IOException {
    return toWritable(Integer.parseInt(value));
  }

  @Override
  protected BooleanWritable toWritable(Integer value) throws IOException {
    writable.set(value.intValue() == 1 ? true : false);
    return writable;
  }

  @Override
  protected BooleanWritable toWritable(Long value) throws IOException {
    return toWritable(value.intValue());
  }

  @Override
  protected BooleanWritable toWritable(Float value) throws IOException {
    return toWritable(value.intValue());
  }

  @Override
  protected BooleanWritable toWritable(Double value) throws IOException {
    return toWritable(value.intValue());
  }
}
