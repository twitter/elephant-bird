package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion from NullWritable to Pig null, and from all Pig types to {@link NullWritable}
 * .
 *
 * @author Andy Schlaikjer
 */
public class NullWritableConverter extends AbstractWritableConverter<NullWritable> {
  public NullWritableConverter() {
    super(NullWritable.get());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.NULL);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return null;
  }

  @Override
  public NullWritable toWritable(Object value) throws IOException {
    return writable;
  }
}
