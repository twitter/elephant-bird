package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the class for all base64 encoded, line-oriented protocol buffer based pig loaders.
 * Data is expected to be one base64 encoded serialized protocol buffer per line. The specific
 * protocol buffer is a template parameter, generally specified by a codegen'd derived class.
 * See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public class LzoProtobufB64LinePigLoader<M extends Message> extends LzoBinaryB64LinePigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LinePigLoader.class);

  private TypeRef<M> typeRef_ = null;
  private ProtobufConverter<M> protoConverter_ = null;

  public LzoProtobufB64LinePigLoader() {
    LOG.info("LzoProtobufB64LineLoader zero-parameter creation");
  }

  public LzoProtobufB64LinePigLoader(String protoClassName) {
    TypeRef<M> typeRef = Protobufs.getTypeRef(protoClassName);
    setTypeRef(typeRef);
    setLoaderSpec(getClass(), new String[]{protoClassName});
  }

  /**
   * Set the type parameter so it doesn't get erased by Java.  Must be called before getNext!
   *
   * @param typeRef
   */
  public void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    protoConverter_ = ProtobufConverter.newInstance(typeRef);
    BinaryConverter<Tuple> converter = new BinaryConverter<Tuple>() {
      public Tuple fromBytes(byte[] messageBuffer) {
        Message message = protoConverter_.fromBytes(messageBuffer);
        if (message != null) {
          return new ProtobufTuple(message);
        }
        return null;
      }

      public byte[] toBytes(Tuple message) {
        throw new RuntimeException("not implemented");
      }
    };

    init(typeRef_.getRawClass().getName(), converter);
  }

  @Override
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return new ProtobufToPig().toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass()));
  }
}
