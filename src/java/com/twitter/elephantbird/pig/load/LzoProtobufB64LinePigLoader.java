package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
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

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufB64LineInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based pig loaders.
 * Data is expected to be one base64 encoded serialized protocol buffer per line. The specific
 * protocol buffer is a template parameter.<br>
 * Initialize with a String argument that represents the full classpath of the protocol buffer class to be loaded.<br>
 * The no-arg constructor will not work and is only there for internal Pig reasons.
 */
public class LzoProtobufB64LinePigLoader<M extends Message> extends LzoBaseLoadFunc implements LoadMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LinePigLoader.class);

  private TypeRef<M> typeRef_ = null;
  private final ProtobufToPig protoToPig_ = new ProtobufToPig();

  private Pair<String, String> protobufsRead;

  public LzoProtobufB64LinePigLoader() {
    LOG.info("LzoProtobufB64LineLoader zero-parameter creation");
  }

  /**
  *
  * @param protoClassName full classpath to the generated Protocol Buffer to be loaded.
  */
  public LzoProtobufB64LinePigLoader(String protoClassName) {
    TypeRef<M> typeRef = PigUtil.getProtobufTypeRef(protoClassName);
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
    String group = "LzoB64Lines of " + typeRef_.getRawClass().getName();
    protobufsRead = new Pair<String, String>(group, "Protobufs Read");
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader_ == null) {
      return null;
    }

    Tuple t = null;
    try {
    	while(reader_.nextKeyValue()){
    	  @SuppressWarnings("unchecked")
        M protoValue = ((ProtobufWritable<M>) reader_.getCurrentValue()).get();
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

  /**
   * NOT IMPLEMENTED
   * @param arg0
   * @param arg1
   * @return
   * @throws IOException
   */
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

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
    if (typeRef_ == null) {
      LOG.error("Protobuf class must be specified before an InputFormat can be created. Do not use the no-argument constructor.");
      throw new IllegalArgumentException("Protobuf class must be specified before an InputFormat can be created. Do not use the no-argument constructor.");
    }
    return LzoProtobufB64LineInputFormat.newInstance(typeRef_);
  }

}
