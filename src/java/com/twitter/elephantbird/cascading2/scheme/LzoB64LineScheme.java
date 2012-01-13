package com.twitter.elephantbird.cascading2.scheme;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.Codecs;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tuple.Fields;
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
  public void sink(HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
    throws IOException {
    OutputCollector collector = sinkCall.getOutput();
    TupleEntry entry = sinkCall.getOutgoingEntry();
    Object message = entry.getTuple().getObject(0);
    byte[] bytes = encodeMessage(message);
    byte[] encodedBytes = getBase64().encode(bytes);
    String encoded = new String(encodedBytes, ENCODING);
    collector.collect(null, new Text(encoded));
  }

  @Override
  public boolean source(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    //Read the next item into length 2 object array:
    Object[] context = sourceCall.getContext();
    if (!sourceCall.getInput().next(context[0], context[1])) {
      return false;
    }
    boolean success = false;
    //We have the next value, decode it:
    Text encoded = (Text) context[1];
    try {
      byte[] bytes = getBase64().decode(encoded.toString().getBytes(ENCODING));
      Object message = decodeMessage(bytes);
      if (message == null) {
        LOG.info("Couldn't decode " + encoded + " " + Arrays.toString(bytes));
      } else {
        //Decoded, and time to hand it over
        sourceCall.getIncomingEntry().getTuple().set(0, message);
        //Only successful exit point is here:
        success = true;
      }
    } catch (java.io.UnsupportedEncodingException e) {
      LOG.info(e.toString());
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.info("Could not decode " + encoded);
    }
    return success;
  }

  /**
  * This sets up the state between succesive calls to source
  */
  @Override
  public void sourcePrepare(HadoopFlowProcess flowProcess,
    SourceCall<Object[], RecordReader> sourceCall) {
    //Hadoop sets a key value pair:
    sourceCall.setContext(new Object[2]);
    sourceCall.getContext()[0] = sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = sourceCall.getInput().createValue();
  }
}
