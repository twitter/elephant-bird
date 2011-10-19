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
   * Class of Writable instance used internally for conversion.
   */
  protected Class<W> writableClass;

  @SuppressWarnings("unchecked")
  public AbstractWritableConverter(W writable) {
    super(writable);
    if (writable != null) {
      writableClass = (Class<W>) writable.getClass();
    }
  }

  public AbstractWritableConverter() {
    super();
  }

  /**
   * For non-null inputs, tests that any existing {@link #writableClass} is assignable from the
   * given class and updates {@link #writableClass} to reference given class.
   *
   * @see WritableConverter#initialize(java.lang.Class)
   */
  @Override
  @SuppressWarnings("unchecked")
  public void initialize(Class<? extends W> writableClass) {
    if (writableClass == null) {
      return;
    }
    if (this.writableClass != null) {
      Preconditions.checkArgument(this.writableClass.isAssignableFrom(writableClass),
          "Existing Writable implementation '%s' is not assignable from class '%s'",
          this.writableClass.getName(), writableClass.getName());
    }
    this.writableClass = (Class<W>) writableClass;
  }

  /**
   * Default implementation returns {@code null}.
   *
   * @see com.twitter.elephantbird.pig.util.WritableConverter#getLoadSchema()
   */
  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    return null;
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
   * Default implementation returns value of {@link #writableClass}.
   *
   * @see com.twitter.elephantbird.pig.util.WritableConverter#getWritableClass()
   */
  @Override
  public Class<W> getWritableClass() throws IOException {
    return writableClass;
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
    if (value == null) {
      return null;
    }
    // route to appropriate method using Pig data type
    switch (DataType.findType(value)) {
      case DataType.BYTEARRAY:
        return toWritable((DataByteArray) value);
      case DataType.CHARARRAY:
        return toWritable((String) value);
      case DataType.INTEGER:
        return toWritable((Integer) value);
      case DataType.LONG:
        return toWritable((Long) value);
      case DataType.FLOAT:
        return toWritable((Float) value);
      case DataType.DOUBLE:
        return toWritable((Double) value);
      case DataType.MAP:
        return toWritable((Map<String, Object>) value);
      case DataType.TUPLE:
        return toWritable((Tuple) value);
      case DataType.BAG:
        return toWritable((DataBag) value);
      default:
        throw new IOException("Pig value class '" + value.getClass().getName() + "' is unsupported");
    }
  }
}
