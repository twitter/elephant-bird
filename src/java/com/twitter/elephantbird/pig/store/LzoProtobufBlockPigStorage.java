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
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
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
public class LzoProtobufBlockPigStorage<M extends Message> extends LzoBaseStoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockPigStorage.class);

  private TypeRef<M> typeRef_;
  private ProtobufWritable<M> writable;
  private ProtobufExtensionRegistry extensionRegistry_;

  public LzoProtobufBlockPigStorage() {}

  public LzoProtobufBlockPigStorage(String protoClassName) {
    this(protoClassName, null);
  }

  public LzoProtobufBlockPigStorage(String protoClassName,
      String extensionRegistryClassName) {
    typeRef_ = Protobufs.getTypeRef(protoClassName);
    if(extensionRegistryClassName != null) {
      extensionRegistry_ = Protobufs.getExtensionRegistry(extensionRegistryClassName);
    }
    writable = ProtobufWritable.newInstance(typeRef_, extensionRegistry_);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void putNext(Tuple f) throws IOException {
    if (f == null) {
      return;
    }
    Builder builder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
    try {
      writable.set((M) PigToProtobuf.tupleToMessage(builder, f, extensionRegistry_));
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
    return new LzoProtobufBlockOutputFormat<M>(typeRef_, extensionRegistry_);
  }

}
