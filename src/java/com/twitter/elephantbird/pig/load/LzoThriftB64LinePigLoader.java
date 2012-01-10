package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.input.LzoRecordReader;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProjectedThriftTupleFactory;
import com.twitter.elephantbird.pig.util.ThriftToPig;

import com.twitter.elephantbird.util.TypeRef;

public class LzoThriftB64LinePigLoader<M extends TBase<?, ?>> extends LzoBaseLoadFunc {

  protected final TypeRef<M> typeRef_;
  private ProjectedThriftTupleFactory<M> tupleTemplate;

  public LzoThriftB64LinePigLoader(String thriftClassName) {
    typeRef_ = PigUtil.getThriftTypeRef(thriftClassName);
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
      tupleTemplate = new ProjectedThriftTupleFactory<M>(typeRef_, requiredFieldList);
    }

    M value = getNextBinaryValue(typeRef_);

    return value != null ?
      tupleTemplate.newTuple(value) : null;
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
                                              throws FrontendException {
    return pushProjectionHelper(requiredFieldList);
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    return new ResourceSchema(ThriftToPig.toSchema(typeRef_.getRawClass()));
  }

  @Override
  public InputFormat<LongWritable, BinaryWritable<M>> getInputFormat() throws IOException {
    return new MultiInputFormat<M>(typeRef_);
  }
}
