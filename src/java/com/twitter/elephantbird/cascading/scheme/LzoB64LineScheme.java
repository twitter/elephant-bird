package com.twitter.elephantbird.cascading.scheme;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.Codecs;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Abstract base class for Base64 encoded text schemes.
 *
 * @author Avi Bryant, Ning Liang
 */
public abstract class LzoB64LineScheme extends LzoTextLine {
  public static final String LINE_FIELD_NAME = "message";
  private static final String ENCODING = "UTF-8";
  private static final Logger LOG = LoggerFactory.getLogger(LzoB64LineScheme.class);

  private transient Base64 base64;

  public LzoB64LineScheme() {
    super(new Fields(LINE_FIELD_NAME));
  }

  protected abstract Object decodeMessage(byte[] bytes);
  protected abstract byte[] encodeMessage(Object message);

  private Base64 getBase64() {
    if (base64 == null) {
      base64 = Codecs.createStandardBase64();
    }
    return base64;
  }

  @Override
  public void sink(TupleEntry entry, OutputCollector collector) throws IOException {
    Object message = entry.getTuple().getObject(0);
    byte[] bytes = encodeMessage(message);
    byte[] encodedBytes = getBase64().encode(bytes);
    String encoded = new String(encodedBytes, ENCODING);
    collector.collect(null, new Text(encoded));
  }

  @Override
  public Tuple source(Object key, Object value) {
    Text encoded = (Text) value;

    try {
      byte[] bytes = getBase64().decode(encoded.toString().getBytes(ENCODING));
      Object message = decodeMessage(bytes);
      if (message == null) {
        LOG.info("Couldn't decode " + encoded + " " + Arrays.toString(bytes));
      } else {
        return new Tuple(message);
      }
    } catch (java.io.UnsupportedEncodingException e) {
      LOG.info(e.toString());
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.info("Could not decode " + encoded);
    }

    return null;
  }
}
