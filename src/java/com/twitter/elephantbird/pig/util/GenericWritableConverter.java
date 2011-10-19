package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion between Pig bytearray ({@link DataByteArray}) and an arbitrary
 * {@link Writable} implementation type. Useful for loading data from a SequenceFile when the key or
 * value must be passed through to output, but otherwise goes untouched by Pig.
 *
 * @author Andy Schlaikjer
 */
public class GenericWritableConverter extends AbstractWritableConverter<Writable> {
  private final DataInputBuffer ibuf = new DataInputBuffer();

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.BYTEARRAY);
    return schema;
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    Preconditions.checkNotNull(schema);
    if (schema.getType() != DataType.BYTEARRAY)
      throw new IOException("Expected Pig type '" + DataType.findTypeName(DataType.BYTEARRAY)
          + "' but found '" + DataType.findTypeName(schema.getType()) + "'");
  }

  @Override
  protected Writable toWritable(DataByteArray value) throws IOException {
    Preconditions.checkNotNull(writable, "Writable is null");
    byte[] bytes = value.get();
    ibuf.reset(bytes, bytes.length);
    writable.readFields(ibuf);
    return writable;
  }
}
