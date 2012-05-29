package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * The base class for a Pig UDF that takes as input a tuple containing a single element, the
 * bytes of a serialized protocol buffer as a DataByteArray.  It outputs the protobuf in
 * expanded form.  The specific protocol buffer is a template parameter, generally specified by a
 * codegen'd derived class. See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 * Alternatly, full class name could be passed to the constructor in Pig:
 * <pre>
 *   DEFINE PersonProtobufBytesToTuple com.twitter.elephantbird.pig.piggybank.ProtobufBytesToTuple('com.twitter.elephantbird.proto.Person');
 *   persons = FOREACH protobufs GENERATE PersonProtobufBytesToTuple($0);
 * </pre>
 */
public class ProtobufBytesToTuple<M extends Message> extends EvalFunc<Tuple> {
  private TypeRef<M> typeRef_ = null;
  private ProtobufExtensionRegistry extensionRegistry_ = null;
  private ProtobufConverter<M> protoConverter_ = null;
  private final ProtobufToPig protoToPig_ = new ProtobufToPig();

  public ProtobufBytesToTuple() {}

  public ProtobufBytesToTuple(String protoClassName) {
    this(PigUtil.<M>getProtobufTypeRef(protoClassName), null);
  }

  public ProtobufBytesToTuple(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public ProtobufBytesToTuple(String protoClassName, String extensionRegistryClassName) {
    this(PigUtil.<M>getProtobufTypeRef(protoClassName), extensionRegistryClassName != null ?
        Protobufs.getExtensionRegistry(extensionRegistryClassName) : null);
  }

  public ProtobufBytesToTuple(TypeRef<M> typeRef, ProtobufExtensionRegistry extensionRegistry) {
    typeRef_ = typeRef;
    extensionRegistry_ = extensionRegistry;
    protoConverter_ = ProtobufConverter.newInstance(typeRef_, extensionRegistry_);
  }

  @Override
  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() < 1) {
      return null;
    }
    try {
      DataByteArray bytes = (DataByteArray) input.get(0);
      M value_ = protoConverter_.fromBytes(bytes.get());
      return new ProtobufTuple(value_, extensionRegistry_);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    return protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass()),
        extensionRegistry_);
  }
}
