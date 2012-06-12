package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;


/**
 * Serializes Pig Tuples into Block encoded protocol buffers.
 * The fields in the pig tuple must correspond exactly to the fields in the protobuf, as
 * no name-matching is performed (that's a TODO).<br>
 *
 *
 * @param <M> Protocol Buffer Message class being serialized
 */
public class LzoProtobufBlockPigStorage<M extends Message> extends BaseStoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockPigStorage.class);

  private TypeRef<M> typeRef_;
  private ProtobufWritable<M> writable;

  public LzoProtobufBlockPigStorage() {}

  public LzoProtobufBlockPigStorage(String protoClassName) {
    TypeRef<M> typeRef = Protobufs.getTypeRef(protoClassName);
    setTypeRef(typeRef);
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    writable = ProtobufWritable.newInstance(typeRef_.getRawClass());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void putNext(Tuple f) throws IOException {
    if (f == null) {
      return;
    }
    Builder builder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
    try {
      writable.set((M) PigToProtobuf.tupleToMessage(builder, f));
      writer.write(null, writable);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public OutputFormat<M, ProtobufWritable<M>> getOutputFormat() throws IOException {
    if (typeRef_ == null) {
      LOG.error("Protobuf class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
      throw new IllegalArgumentException("Protobuf class must be specified before an OutputFormat can be created. Do not use the no-argument constructor.");
    }
    return new LzoProtobufBlockOutputFormat<M>(typeRef_);
  }

}
