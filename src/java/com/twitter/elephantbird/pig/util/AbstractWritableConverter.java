package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Writable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Base class providing default implementations for WritableConverter methods.
 *
 * @author Andy Schlaikjer
 */
public abstract class AbstractWritableConverter<W extends Writable> extends WritableStoreCaster<W>
    implements WritableConverter<W> {
  /**
   * Default implementation does nothing.
   *
   * @see WritableConverter#initialize(java.lang.Class)
   */
  @Override
  public void initialize(Class<? extends W> writableClass) {
  }

  /**
   * Default implementation returns {@code null}.
   *
   * @see com.twitter.elephantbird.pig.util.WritableConverter#getLoadSchema()
   */
  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.BYTEARRAY);
    return schema;
  }

  /**
   * Default implementation passes raw bytes through to Pig without deserializing Writable data.
   *
   * @see WritableConverter#bytesToObject(org.apache.pig.data.DataByteArray)
   */
  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return dataByteArray;
  }

  /**
   * Default implementation does nothing.
   *
   * @see WritableConverter#checkStoreSchema(org.apache.pig.ResourceSchema.ResourceFieldSchema)
   */
  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
  }

  /**
   * Default implementation routes to one of the other {@code toWritable} methods based on the type
   * of the input Pig value.
   *
   * @see WritableConverter#toWritable(java.lang.Object, org.apache.hadoop.io.Writable)
   */
  @Override
  @SuppressWarnings("unchecked")
  public W toWritable(Object value) throws IOException {
    Preconditions.checkNotNull(value);
    // route to appropriate method using Pig data type
    byte type = DataType.findType(value);
    switch (type) {
      case DataType.BYTEARRAY:
        return toWritable((DataByteArray) value, false);
      case DataType.CHARARRAY:
        return toWritable((String) value, false);
      case DataType.INTEGER:
        return toWritable((Integer) value, false);
      case DataType.LONG:
        return toWritable((Long) value, false);
      case DataType.FLOAT:
        return toWritable((Float) value, false);
      case DataType.DOUBLE:
        return toWritable((Double) value, false);
      case DataType.MAP:
        return toWritable((Map<String, Object>) value, false);
      case DataType.TUPLE:
        return toWritable((Tuple) value, false);
      case DataType.BAG:
        return toWritable((DataBag) value, false);
      case DataType.ERROR:
        throw new IOException("Failed to find Pig type for class '" + value.getClass().getName()
            + "'");
    }
    throw new IOException("Pig type '" + DataType.findTypeName(type) + "' is unsupported");
  }
}
