package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.piggybank.ThriftToPig;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based pig loaders.
 * Data is expected to be one base64 encoded serialized protocol buffer per line. The specific
 * protocol buffer is a template parameter, generally specified by a codegen'd derived class.
 * See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public class LzoThriftB64LinePigLoader<M extends TBase<?>> extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftB64LinePigLoader.class);

  private final TypeRef<M> typeRef_;
  private final ThriftConverter<M> protoConverter_;
  private final Base64 base64_ = new Base64();
  private final ThriftToPig<M> thriftToPig_;

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  protected enum Counters { LinesRead, ThriftStructsRead }

  public LzoThriftB64LinePigLoader(String thriftClassName) {
    typeRef_ = ThriftUtils.getTypeRef(thriftClassName);
    protoConverter_ = ThriftConverter.newInstance(typeRef_);
    thriftToPig_ =  ThriftToPig.newInstance(typeRef_);
    setLoaderSpec(getClass(), new String[]{thriftClassName});
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
      incrCounter(Counters.LinesRead, 1L);
      M protoValue = protoConverter_.fromBytes(base64_.decode(line.getBytes("UTF-8")));
      if (protoValue != null) {
        try {
          t = thriftToPig_.getPigTuple(protoValue);
          incrCounter(Counters.ThriftStructsRead, 1L);
          break;
        } catch (TException e) {
          LOG.warn("ThriftToTuple error :", e); // may be struct mismatch
        }
      }
    }

    return t;
  }

  @Override
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return ThriftToPig.toSchema(typeRef_.getRawClass());
  }
}
