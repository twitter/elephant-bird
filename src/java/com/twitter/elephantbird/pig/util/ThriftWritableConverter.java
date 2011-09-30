package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.pig.store.SequenceFileStorage;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Supports conversion between Pig {@link Tuple} and {@link ThriftWritable} types. For example, say
 * we have thrift type {@code Person}. We can use {@link ThriftWritableConverter} and
 * {@link SequenceFileStorage} to convert Tuple data to {@link ThriftWritable}{@code <Person>}
 * instances and store these as values in a {@link SequenceFile}:
 *
 * <pre>
 * -- assume that we identify Person instances by integer id
 * people = LOAD '$data' AS (id: int, person: ());
 *
 * STORE people INTO '$output' USING com.twitter.elephantbird.pig.store.SequenceFileStorage (
 *   '-t org.apache.hadoop.io.IntWritable -c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-t com.twitter.elephantbird.mapreduce.io.ThriftWritable -c com.twitter.elephantbird.pig.util.ThriftWritableConverter Person'
 * );
 * </pre>
 *
 * Notice that we supply the name of our thrift {@code Person} class as an extra argument to
 * {@code -c com.twitter.elephantbird.pig.util.ThriftWritableConverter} above. This ensures the
 * ThriftWritableConverter instance created by the SequenceFileStorage instance knows what thrift
 * type it's dealing with.
 *
 * We can also load {@code ThriftWritable<Person>} data from a SequenceFile and convert back to
 * Tuples:
 *
 * <pre>
 * people = LOAD '$data' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 *   '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-c com.twitter.elephantbird.pig.util.ThriftWritableConverter Person'
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 */
public class ThriftWritableConverter<M extends TBase<?, ?>> extends
    AbstractWritableConverter<ThriftWritable<M>> {
  protected final TypeRef<M> typeRef;
  protected final ThriftToPig<M> thriftToPig;
  protected final PigToThrift<M> pigToThrift;
  protected Class<? extends ThriftWritable<M>> writableClass;

  public ThriftWritableConverter(String thriftClassName) {
    Preconditions.checkNotNull(thriftClassName);
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    thriftToPig = ThriftToPig.newInstance(typeRef);
    pigToThrift = PigToThrift.newInstance(typeRef);
  }

  @Override
  public void initialize(Class<? extends ThriftWritable<M>> writableClass) {
    this.writableClass = writableClass;
    this.writable = createWritable();
  }

  private ThriftWritable<M> createWritable() {
    if (writableClass != null && ThriftWritable.class.isAssignableFrom(writableClass)) {
      try {
        ThriftWritable<M> writable = writableClass.newInstance();
        writable.setConverter(typeRef.getRawClass());
        return writable;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return ThriftWritable.newInstance(typeRef.getRawClass());
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
  protected Tuple toTuple(ThriftWritable<M> writable, ResourceFieldSchema schema)
      throws IOException {
    return thriftToPig.getPigTuple(writable.get());
  }

  @Override
  protected ThriftWritable<M> toWritable(Tuple value, boolean newInstance) throws IOException {
    ThriftWritable<M> out = this.writable;
    if (newInstance) {
      out = createWritable();
    }
    out.set(pigToThrift.getThriftObject(value));
    return out;
  }
}
