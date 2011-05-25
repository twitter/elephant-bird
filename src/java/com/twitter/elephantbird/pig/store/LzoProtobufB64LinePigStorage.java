package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
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
  private final Base64 base64_ = new Base64();
  private final PigToProtobuf pigToProto_ = new PigToProtobuf();

  public LzoProtobufB64LinePigStorage() {}

  public LzoProtobufB64LinePigStorage(String protoClassName) {
    TypeRef<M> typeRef = Protobufs.getTypeRef(protoClassName);
    setTypeRef(typeRef);
    setStorageSpec(getClass(), new String[]{protoClassName});
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple f) throws IOException {
    if (f == null) {
      return;
    }
    Builder builder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
    try {
      writer.write(NullWritable.get(),
          PigToProtobuf.tupleToMessage(builder, f));
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
