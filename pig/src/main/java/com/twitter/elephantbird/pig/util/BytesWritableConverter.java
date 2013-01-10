package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

/**
 * Supports conversion between Pig bytearray and {@link org.apache.hadoop.io.BytesWritable}.
 *
 * @author Andy Schlaikjer
 */
public class BytesWritableConverter extends AbstractWritableConverter<BytesWritable> {
  private final DataInputBuffer in = new DataInputBuffer();

  public BytesWritableConverter() {
    super(new BytesWritable());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.BYTEARRAY);
    return schema;
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    byte[] bytes = dataByteArray.get();
    // test leading 4 bytes encode run length of rest of data
    in.reset(bytes, bytes.length);
    int length = in.readInt();
    if (length != bytes.length - 4) {
      throw new IOException(String.format(
          "Int value '%d' of leading four bytes does not match run length of data '%d'",
          length, bytes.length - 4));
    }
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
    // avoid array copy at the sake of new BytesWritable
    return new BytesWritable(value.get());
  }
}
