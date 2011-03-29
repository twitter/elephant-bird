package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.util.Base64;
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

  private TypeRef<M> typeRef_;
  private Message msgObj; // for newBuilder()

  protected LzoProtobufB64LinePigStorage(){}

  public LzoProtobufB64LinePigStorage(String protoClassName) {
    TypeRef<M> typeRef = Protobufs.getTypeRef(protoClassName);
    setTypeRef(typeRef);
  }

  protected void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    msgObj =  Protobufs.getMessageBuilder(typeRef_.getRawClass()).build();
  }

  @Override
public void putNext(Tuple f) throws IOException {
    if (f == null) return;
    Message message = PigToProtobuf.tupleToMessage(msgObj.newBuilderForType(), f);
    os_.write(Base64.encodeBytesToBytes(message.toByteArray()));
    os_.write(Protobufs.NEWLINE_UTF8_BYTE);
  }

}
