package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.output.LzoThriftB64LineOutputFormat;
import com.twitter.elephantbird.pig.store.LzoBaseStoreFunc;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Serializes Pig Tuples into Base-64 encoded, line-delimited Thrift objects.
 * The fields in the pig tuple must correspond exactly to the fields in
 * the Thrift object, as no name-matching is performed (names of the tuple
 * fields are not currently accessible to a StoreFunc. It will be in 0.7,
 * so something more flexible will be possible)
 */
public class LzoThriftB64LinePigStorage<T extends TBase<?, ?>> extends LzoBaseStoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseStoreFunc.class);

  private TypeRef<T> typeRef;
  private PigToThrift<T> pigToThrift;

  public LzoThriftB64LinePigStorage(String thriftClassName) {
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
    pigToThrift = PigToThrift.newInstance(typeRef);
    setStorageSpec(getClass(), new String[]{thriftClassName});
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple f) throws IOException {
    if (f == null) return;
    try {
      writer.write(NullWritable.get(),
          pigToThrift.getThriftObject(f));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    if (typeRef == null) {
      LOG.error("Thrift class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
      throw new IllegalArgumentException("Thrift class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
    }
    return new LzoThriftB64LineOutputFormat<T>();
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    super.setStoreLocation(location, job);
    LzoThriftB64LineOutputFormat.getOutputFormatClass(typeRef.getRawClass(), job.getConfiguration());
  }
}
