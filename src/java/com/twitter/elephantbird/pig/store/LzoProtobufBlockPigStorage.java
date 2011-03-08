package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import java.io.OutputStream;


/**
 * Serializes Pig Tuples into Block encodedprotocol buffers.
 * The fields in the pig tuple must correspond exactly to the fields in the protobuf, as
 * no name-matching is performed (names of the tuple fields are not currently accessible to
 * a StoreFunc. It will be in 0.7, so something more flexible will be possible)
 *
 * @param <M> Protocol Buffer Message class being serialized
 */
public class LzoProtobufBlockPigStorage<M extends Message> extends LzoBaseStoreFunc {

  private TypeRef<M> typeRef_;
  private Builder builder_;
  protected ProtobufBlockWriter<M> writer_ = null;
  private int numRecordsPerBlock_ = 10000;

  protected LzoProtobufBlockPigStorage() {
  }

  public LzoProtobufBlockPigStorage(String protoClassName) {
    TypeRef<M> typeRef = Protobufs.getTypeRef(protoClassName);
    setTypeRef(typeRef);
  }

  @Override
  public void bindTo(OutputStream os) throws IOException {
		super.bindTo(os);
		writer_ = new ProtobufBlockWriter<M>(os_, typeRef_.getRawClass(), numRecordsPerBlock_);
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    builder_ = Protobufs.getMessageBuilder(typeRef_.getRawClass());
  }

  @SuppressWarnings("unchecked")
  public void putNext(Tuple f) throws IOException {
    if (f == null) return;
	  writer_.write((M)PigToProtobuf.tupleToMessage(builder_, f));
  }

	@Override
	public void finish() throws IOException {
    if (writer_ != null) {
      writer_.close();
    }
  }
}
