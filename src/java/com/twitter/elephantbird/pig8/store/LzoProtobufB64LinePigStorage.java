package com.twitter.elephantbird.pig8.store;

import java.io.IOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.pig8.util.PigToProtobuf;
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
public abstract class LzoProtobufB64LinePigStorage<M extends Message> extends LzoBaseStoreFunc {

  private TypeRef<M> typeRef_;
  private final Base64 base64_ = new Base64();
  private final PigToProtobuf pigToProto_ = new PigToProtobuf();

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
      writer.write(null,
          base64_.encode(pigToProto_.tupleToMessage(builder, f).toByteArray()).toString()+"\n");
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
