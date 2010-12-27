package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based pig loaders.
 * Data is expected to be one base64 encoded serialized protocol buffer per line. The specific
 * protocol buffer is a template parameter, generally specified by a codegen'd derived class.
 * See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public class LzoProtobufB64LinePigLoader<M extends Message> extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LinePigLoader.class);

  private TypeRef<M> typeRef_ = null;
  private ProtobufConverter<M> protoConverter_ = null;
  private final Base64 base64_ = new Base64();
  private final ProtobufToPig protoToPig_ = new ProtobufToPig();

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  private Pair<String, String> linesRead;
  private Pair<String, String> protobufsRead;
  private Pair<String, String> protobufErrors;

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
    String group = "LzoB64Lines of " + typeRef_.getRawClass().getName();
    linesRead = new Pair<String, String>(group, "Lines Read");
    protobufsRead = new Pair<String, String>(group, "Protobufs Read");
    protobufErrors = new Pair<String, String>(group, "Errors");
  }

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first record of each split and count on a different
    // instance to read it.  The only split this doesn't work for is the first.
    if (!atFirstRecord) {
      getNext();
    }
  }

  @Override
  protected boolean verifyStream() throws IOException {
    return is_ != null;
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  public Tuple getNext() throws IOException {
    if (!verifyStream()) {
      return null;
    }

    String line;
    Tuple t = null;
    while ((line = is_.readLine(UTF8, RECORD_DELIMITER)) != null) {
      incrCounter(linesRead, 1L);
      M protoValue = protoConverter_.fromBytes(base64_.decode(line.getBytes("UTF-8")));
      if (protoValue != null) {
        t = new ProtobufTuple(protoValue);
        incrCounter(protobufsRead, 1L);
        break;
      } else {
        incrCounter(protobufErrors, 1L);
      }
    }

    return t;
  }

  @Override
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass()));
  }
}
