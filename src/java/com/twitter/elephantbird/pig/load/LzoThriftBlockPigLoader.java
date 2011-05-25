package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Pair;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.TypeRef;


public class LzoThriftBlockPigLoader<M extends TBase<?, ?>> extends LzoBaseLoadFunc implements LoadMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftBlockPigLoader.class);

  private final TypeRef<M> typeRef_;
  private final ThriftToPig<M> thriftToPig_;
  private final Pair<String, String> thriftErrors;

  public LzoThriftBlockPigLoader(String thriftClassName) {
    typeRef_ = PigUtil.getThriftTypeRef(thriftClassName);
    thriftToPig_ =  ThriftToPig.newInstance(typeRef_);

    String group = "LzoBlocks of " + typeRef_.getRawClass().getName();
    thriftErrors = new Pair<String, String>(group, "Errors");

    setLoaderSpec(getClass(), new String[]{thriftClassName});
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Tuple getNext() throws IOException {
    if (reader_ == null) {
      return null;
    }

    M value;
    try {
      if (reader_.nextKeyValue()) {
        value = ((ThriftWritable<M>) reader_.getCurrentValue()).get();
        return thriftToPig_.getPigTuple(value);
      }
    } catch (InterruptedException e) {
      LOG.error("InterruptedException encountered, bailing.", e);
      throw new IOException(e);
    }
    return null;
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    return new ResourceSchema(ThriftToPig.toSchema(typeRef_.getRawClass()));
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
      try {
        return LzoThriftBlockInputFormat.getInputFormatClass(typeRef_.getRawClass(), jobConf).newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      }
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
    return null;
  }

  /**
   * NOT IMPLEMENTED
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
