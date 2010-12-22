package com.twitter.elephantbird.pig8.load;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.twitter.elephantbird.pig8.util.ProtobufToPig;
import com.twitter.elephantbird.pig8.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based pig loaders.
 * Data is expected to be one base64 encoded serialized protocol buffer per line. The specific
 * protocol buffer is a template parameter, generally specified by a codegen'd derived class.
 * See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 */

public abstract class LzoProtobufB64LinePigLoader<M extends Message> extends LzoBaseLoadFunc implements LoadMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LinePigLoader.class);

  private TypeRef<M> typeRef_ = null;
  private Function<byte[], M> protoConverter_ = null;
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

  /**
   * Set the type parameter so it doesn't get erased by Java.  Must be called before getNext!
   *
   * @param typeRef
   */
  public void setTypeRef(TypeRef<M> typeRef) {
    typeRef_ = typeRef;
    protoConverter_ = Protobufs.getProtoConverter(typeRef.getRawClass());
    String group = "LzoB64Lines of " + typeRef_.getRawClass().getName();
    linesRead = new Pair<String, String>(group, "Lines Read");
    protobufsRead = new Pair<String, String>(group, "Protobufs Read");
    protobufErrors = new Pair<String, String>(group, "Errors");
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader_ == null) {
      return null;
    }

    String line;
    Tuple t = null;
    try {
    	while(reader_.nextKeyValue()){
    	  M protoValue = (M) reader_.getCurrentValue();
    	  if (protoValue != null) {
    	    t = new ProtobufTuple(protoValue);
    	    incrCounter(protobufsRead, 1L);
    	    break;
    	  }
    	}
    } catch (InterruptedException e) {
		  int errCode = 6018;
		  String errMsg = "Error while reading input";
		  throw new ExecException(errMsg, errCode,
				  PigException.REMOTE_ENVIRONMENT, e);
	  }
    return t;
  }

  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    return new ResourceSchema(protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass())));

  }

  /** NOT IMPLEMENTED
   *
   */
  @Override
  public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
    return null;
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public void setPartitionFilter(Expression arg0) throws IOException {

  }

}
