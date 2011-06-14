package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Serializes Pig Tuples into Base-64 encoded, line-delimited protocol buffers.
 * The fields in the pig tuple must correspond exactly to the fields in the protobuf, as
 * no name-matching is performed (names of the tuple fields are not currently accessible to
 * a StoreFunc. It will be in 0.7, so something more flexible will be possible)
 *
 * @param <M> Protocol Buffer Message class being serialized
 */
public class LzoProtobufB64LinePigStorage<M extends Message> extends LzoBaseStoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LinePigStorage.class);

  private TypeRef<M> typeRef_;
  private ProtobufWritable<M> writable;

  public LzoProtobufB64LinePigStorage() {}

  public LzoProtobufB64LinePigStorage(String protoClassName) {
    TypeRef<M> typeRef = Protobufs.getTypeRef(protoClassName);
    setTypeRef(typeRef);
    setStorageSpec(getClass(), new String[]{protoClassName});
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    writable = ProtobufWritable.newInstance(typeRef.getRawClass());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple f) throws IOException {
    if (f == null) {
      return;
    }
    Builder builder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
    try {
      writable.set((M) PigToProtobuf.tupleToMessage(builder, f));
      writer.write(NullWritable.get(), writable);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    if (typeRef_ == null) {
      LOG.error("Protobuf class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
      throw new IllegalArgumentException("Protobuf class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
    }
    return new LzoProtobufB64LineOutputFormat<M>();
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    super.setStoreLocation(location, job);
    LzoProtobufB64LineOutputFormat.getOutputFormatClass(typeRef_.getRawClass(), job.getConfiguration());
  }

}
