package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

public class LzoThriftB64LinePigLoader<M extends TBase<?, ?>> extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftB64LinePigLoader.class);

  private final TypeRef<M> typeRef_;
  private final ThriftConverter<M> converter_;
  private final Base64 base64_ = new Base64();
  private final ThriftToPig<M> thriftToPig_;

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  private Pair<String, String> linesRead;
  private Pair<String, String> thriftStructsRead;
  private Pair<String, String> thriftErrors;

  public LzoThriftB64LinePigLoader(String thriftClassName) {
    typeRef_ = ThriftUtils.getTypeRef(thriftClassName);
    converter_ = ThriftConverter.newInstance(typeRef_);
    thriftToPig_ =  ThriftToPig.newInstance(typeRef_);

    String group = "LzoB64Lines of " + typeRef_.getRawClass().getName();
    linesRead = new Pair<String, String>(group, "Lines Read");
    thriftStructsRead = new Pair<String, String>(group, "Thrift Structs");
    thriftErrors = new Pair<String, String>(group, "Errors");

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
      incrCounter(linesRead, 1L);
      M value = converter_.fromBytes(base64_.decode(line.getBytes("UTF-8")));
      if (value != null) {
        try {
          t = thriftToPig_.getPigTuple(value);
          incrCounter(thriftStructsRead, 1L);
          break;
        } catch (TException e) {
          incrCounter(thriftErrors, 1L);
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
