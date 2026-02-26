package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Supports conversion from Text to Pig chararray and numeric types, and from all Pig types to
 * {@link Text} via {@link Object#toString()}.
 *
 * @author Andy Schlaikjer
 */
public class TextConverter extends AbstractWritableConverter<Text> {
  public TextConverter() {
    super(new Text());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.CHARARRAY);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToCharArray(dataByteArray.get());
  }

  @Override
  protected String toCharArray(Text writable) throws IOException {
    return writable.toString();
  }

  @Override
  protected Integer toInteger(Text writable) throws IOException {
    return Integer.parseInt(writable.toString());
  }

  @Override
  protected Long toLong(Text writable) throws IOException {
    return Long.parseLong(writable.toString());
  }

  @Override
  protected Float toFloat(Text writable) throws IOException {
    return Float.parseFloat(writable.toString());
  }

  @Override
  protected Double toDouble(Text writable) throws IOException {
    return Double.parseDouble(writable.toString());
  }

  @Override
  protected Text toWritable(DataByteArray value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(String value) throws IOException {
    writable.set(value.toString());
    return writable;
  }

  @Override
  protected Text toWritable(Integer value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(Long value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(Float value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(Double value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(Map<String, Object> value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(Tuple value) throws IOException {
    return toWritable(value.toString());
  }

  @Override
  protected Text toWritable(DataBag value) throws IOException {
    return toWritable(value.toString());
  }
}
