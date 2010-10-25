package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.mapreduce.RecordWriter;


/**
 * Serializes Pig Tuples into Block encodedprotocol buffers.
 * The fields in the pig tuple must correspond exactly to the fields in the protobuf, as
 * no name-matching is performed (names of the tuple fields are not currently accessible to
 * a StoreFunc. It will be in 0.7, so something more flexible will be possible)
 *
 * @param <M> Protocol Buffer Message class being serialized
 */
public abstract class LzoProtobufBlockPigStorage<M extends Message> extends LzoBaseStoreFunc {

  private TypeRef<M> typeRef_;
  private final PigToProtobuf pigToProto_ = new PigToProtobuf();
  protected ProtobufBlockWriter writer_ = null;
  private final int numRecordsPerBlock_ = 10000;

  @Override
  public void prepareToWrite(RecordWriter writer) {
    writer_ = new ProtobufBlockWriter(os_, typeRef_.getRawClass(), numRecordsPerBlock_);
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
  }

  @Override
  public void putNext(Tuple f) throws IOException {
    if (f == null) {
      return;
    }
    Builder builder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
    writer_.write(pigToProto_.tupleToMessage(builder, f));
  }

  @Override
  public void finish() throws IOException {
    if (writer_ != null) {
      writer_.close();
    }
  }
}
