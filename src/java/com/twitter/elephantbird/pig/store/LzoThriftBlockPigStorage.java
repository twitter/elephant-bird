package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;
import java.io.OutputStream;

/**
 * Serializes Pig Tuples into Block encoded Thrift objects.
 * The fields in the pig tuple must correspond exactly to the fields in
 * Thrift struct, as no name-matching is performed (names of the tuple
 * fields are not currently accessible to a StoreFunc.
 * It will be in 0.7, so something more flexible will be possible)
 */
public class LzoThriftBlockPigStorage<T extends TBase<?, ?>> extends LzoBaseStoreFunc {

  private TypeRef<T> typeRef;
  private ThriftBlockWriter<T> writer;
  private PigToThrift<T> pigToThrift;
  private int numRecordsPerBlock = 10000; // is this too high?

  public LzoThriftBlockPigStorage(String thriftClassName) {
    // TODO : use PigUtil once pull request #36 is committed.
    typeRef = ThriftUtils.getTypeRef(thriftClassName);
    pigToThrift = PigToThrift.newInstance(typeRef);
  }

  @Override
  public void bindTo(OutputStream os) throws IOException {
    super.bindTo(os);
    writer = new ThriftBlockWriter<T>(os_, typeRef.getRawClass(), numRecordsPerBlock);
  }

  @Override
  public void putNext(Tuple f) throws IOException {
    if (f == null) return;
    writer.write(pigToThrift.getThriftObject(f));
  }

  @Override
  public void finish() throws IOException {
    if (writer != null) {
      writer.close();
    }
    super.finish();
  }
}
