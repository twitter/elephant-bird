package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.RCFileProtobufOutputFormat;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * StoreFunc for storing Protobuf messages in RCFiles. <p>
 *
 * @see {@link RCFileProtobufOutputFormat}
 */
public class RCFileProtobufPigStorage extends BaseStoreFunc {
  // add stats?

  private TypeRef<? extends Message> typeRef;
  private Builder msgBuilder;
  private ProtobufWritable<Message> writable;

  public RCFileProtobufPigStorage(String protoClassName) {
    typeRef = Protobufs.getTypeRef(protoClassName);
    msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
    writable = ProtobufWritable.newInstance(Message.class);
  }

  @Override @SuppressWarnings("unchecked")
  public OutputFormat getOutputFormat() throws IOException {
    return new RCFileProtobufOutputFormat(typeRef);
  }

  public void putNext(Tuple t) throws IOException {
    Message msg = PigToProtobuf.tupleToMessage(msgBuilder.clone(), t);
    writable.set(msg);
    writeRecord(null, writable);
  }
}
