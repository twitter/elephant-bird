package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Supports conversion between Pig Tuple and Thrift types.
 *
 * @author Andy Schlaikjer
 */
public class ThriftWritableConverter<M extends TBase<?, ?>, W extends ThriftWritable<M>> extends
    AbstractWritableConverter<W> {
  protected final TypeRef<M> typeRef;
  protected final ThriftToPig<M> thriftToPig;
  protected final PigToThrift<M> pigToThrift;
  protected Class<? extends W> writableClass;

  public ThriftWritableConverter(String thriftClassName) {
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    thriftToPig = ThriftToPig.newInstance(typeRef);
    pigToThrift = PigToThrift.newInstance(typeRef);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize(Class<? extends W> writableClass) {
    try {
      this.writableClass = writableClass;
      if (this.writableClass != null) {
        this.writable = writableClass.newInstance();
      } else {
        this.writable = (W) ThriftWritable.newInstance(typeRef.getRawClass());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    return new ResourceFieldSchema(new FieldSchema(null,
        ThriftToPig.toSchema(typeRef.getRawClass())));
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToTuple(dataByteArray.get(), null);
  }

  @Override
  protected Tuple toTuple(W writable, ResourceFieldSchema schema) throws IOException {
    return thriftToPig.getPigTuple(writable.get());
  }

  @Override
  protected W toWritable(Tuple value, boolean newInstance) throws IOException {
    W out = this.writable;
    if (newInstance) {
      try {
        out = writableClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    out.set(pigToThrift.getThriftObject(value));
    return out;
  }
}
