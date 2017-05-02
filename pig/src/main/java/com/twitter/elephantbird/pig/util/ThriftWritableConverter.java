package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.thrift.TBase;

import com.google.common.base.Preconditions;
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
 *   '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-c com.twitter.elephantbird.pig.util.ThriftWritableConverter Person'
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
  protected final Schema expectedSchema;
  protected final PigToThrift<M> pigToThrift;

  public ThriftWritableConverter(String thriftClassName) {
    super(new ThriftWritable<M>());
    Preconditions.checkNotNull(thriftClassName);
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    thriftToPig = ThriftToPig.newInstance(typeRef);
    expectedSchema = thriftToPig.toSchema();
    pigToThrift = PigToThrift.newInstance(typeRef);
    writable.setConverter(typeRef.getRawClass());
  }

  @Override
  public void initialize(Class<? extends ThriftWritable<M>> writableClass) throws IOException {
    if (writableClass == null) {
      return;
    }
    super.initialize(writableClass);
    writable.setConverter(typeRef.getRawClass());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    return new ResourceFieldSchema(new FieldSchema(null, expectedSchema));
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    Preconditions.checkNotNull(schema, "Schema is null");
    Preconditions.checkArgument(DataType.TUPLE == schema.getType(),
        "Expected schema type '%s' but found type '%s'", DataType.findTypeName(DataType.TUPLE),
        DataType.findTypeName(schema.getType()));
    ResourceSchema childSchema = schema.getSchema();
    Preconditions.checkNotNull(childSchema, "Child schema is null");
    Schema actualSchema = Schema.getPigSchema(childSchema);
    Preconditions.checkArgument(Schema.equals(expectedSchema, actualSchema, false, true),
        "Expected store schema '%s' but found schema '%s'", expectedSchema, actualSchema);
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
  protected ThriftWritable<M> toWritable(Tuple value) throws IOException {
    writable.set(pigToThrift.getThriftObject(value));
    return writable;
  }
}
