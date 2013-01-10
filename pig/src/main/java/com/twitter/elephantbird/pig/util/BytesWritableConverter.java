package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion between Pig bytearray and {@link org.apache.hadoop.io.BytesWritable}.
 *
 * @author Andy Schlaikjer
 */
public class BytesWritableConverter extends AbstractWritableConverter<BytesWritable> {
  public BytesWritableConverter() {
    super(new BytesWritable());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.INTEGER);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    byte[] bytes = dataByteArray.get();
    // strip leading 4 bytes which encode run length of data
    return new DataByteArray(bytes, 4, bytes.length);
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    switch (schema.getType()) {
      case DataType.BYTEARRAY:
        return;
    }
    throw new IOException("Pig type '" + DataType.findTypeName(schema.getType()) + "' unsupported");
  }

  @Override
  protected BytesWritable toWritable(DataByteArray value) throws IOException {
    byte[] bytes = value.get();
    writable.set(bytes, 0, bytes.length);
    return writable;
  }
}
