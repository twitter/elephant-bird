package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoRecordReader;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProjectedThriftTupleFactory;
import com.twitter.elephantbird.pig.util.ThriftToPig;

import com.twitter.elephantbird.util.TypeRef;

public class ThriftPigLoader<M extends TBase<?, ?>> extends LzoBaseLoadFunc {
  static final Logger LOG = LoggerFactory.getLogger(ThriftPigLoader.class);

  protected final TypeRef<M> typeRef;
  private ProjectedThriftTupleFactory<M> tupleTemplate;

  public ThriftPigLoader(String thriftClassName) {
    typeRef = PigUtil.getThriftTypeRef(thriftClassName);
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   *<p>
   * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
   * for more information on error handling.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (tupleTemplate == null) {
      tupleTemplate = new ProjectedThriftTupleFactory<M>(typeRef, requiredFieldList);
    }

    M value = getNextBinaryValue(typeRef);

    return value != null ?
      tupleTemplate.newTuple(value) : null;
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
                                              throws FrontendException {
    return pushProjectionHelper(requiredFieldList);
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    super.setLocation(location, job);
    if (job != null) {
      ThriftToPig.setConversionProperties(HadoopCompat.getConfiguration(job));
    }
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    // getSchema usually should only be called after setLocation, but it is not always enforced.
    if (job != null) {
      ThriftToPig.setConversionProperties(HadoopCompat.getConfiguration(job));
    }
    return new ResourceSchema(ThriftToPig.toSchema(typeRef.getRawClass()));
  }

  @Override
  public InputFormat<LongWritable, BinaryWritable<M>> getInputFormat() throws IOException {
    return new MultiInputFormat<M>(typeRef);
  }
}
