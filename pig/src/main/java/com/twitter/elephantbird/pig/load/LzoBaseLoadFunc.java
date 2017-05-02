package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Provides common functionality & helpers required by all elephantbird loaders.
 * TODO: Rename "BaseLoadFunc" as this class has nothing to do with LZO.
 */
public abstract class LzoBaseLoadFunc extends LoadFunc implements LoadMetadata, LoadPushDown {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseLoadFunc.class);

  @SuppressWarnings("rawtypes")
  protected RecordReader reader;

  // Making accessing Hadoop counters from Pig slightly more convenient.
  private final PigCounterHelper counterHelper_ = new PigCounterHelper();

  protected Configuration jobConf;
  protected String contextSignature;
  protected static final String projectionKey = "LzoBaseLoadFunc_projectedFields";

  protected RequiredFieldList requiredFieldList = null;

  /**
   * Construct a new load func.
   */
  public LzoBaseLoadFunc() {
  }

  /**
   * A convenience function for working with Hadoop counter objects from load functions.  The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(String group, String counter, long incr) {
    counterHelper_.incrCounter(group, counter, incr);
  }

  /**
   * A convenience function for working with Hadoop counter objects from load functions.  The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(Enum<?> key, long incr) {
    counterHelper_.incrCounter(key, incr);
  }

  /** same as incrCounter(pair.first, pair.second, incr). */
  protected void incrCounter(Pair<String, String> groupCounterPair, long incr) {
    counterHelper_.incrCounter(groupCounterPair.first, groupCounterPair.second, incr);
  }

  /** UDF properties for this class based on context signature */
  protected Properties getUDFProperties() {
    return UDFContext.getUDFContext()
        .getUDFProperties(this.getClass(), new String[] {contextSignature});
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
    this.jobConf = HadoopCompat.getConfiguration(job);

    String projectedFields = getUDFProperties().getProperty(projectionKey);
    if (projectedFields != null) {
      requiredFieldList =
        (RequiredFieldList) ObjectSerializer.deserialize(projectedFields);
    }
  }

  /**
   * A utility method for loaders that read {@link BinaryWritable} values.
   */
  protected <M> M getNextBinaryValue(TypeRef<M> typeRef) throws IOException {
    //typeRef is just to help compiler resolve the type at compile time.
    try {
      if (reader != null && reader.nextKeyValue()) {
        @SuppressWarnings("unchecked")
        BinaryWritable<M> writable = (BinaryWritable<M>)reader.getCurrentValue();
        return writable.get();
      }
    } catch (InterruptedException e) {
      LOG.error("InterruptedException encountered, bailing.", e);
      throw new IOException(e);
    }

    return null;
  }

  // LoadPushDown implementation:

  @Override
  public List<OperatorSet> getFeatures() {
    return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(
      RequiredFieldList requiredFieldList) throws FrontendException {
    // the default implementation disables the feature.
    // a projection needs to be explicitly supported by sub classes.
    return null;
  }

  /**
   * A helper method for implementing
   * {@link LoadPushDown#pushProjection(RequiredFieldList)}. <p>
   *
   * Stores requiredFieldList in context. The requiredFields are read from
   * context on the backend (in side {@link #setLocation(String, Job)}).
   */
  protected RequiredFieldResponse pushProjectionHelper(
                                          RequiredFieldList requiredFieldList)
                                          throws FrontendException {
    try {
      getUDFProperties().setProperty(projectionKey,
                                     ObjectSerializer.serialize(requiredFieldList));
    } catch (IOException e) { // not expected
      throw new FrontendException(e);
    }

    return new RequiredFieldResponse(true);
  }

  @Override
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
      this.reader = reader;
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    // most loaders are expected to override this.
    return null;
  }

  /*
   * NOT IMPLEMENTED
   */
  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
    return null;
  }

  /*
   * NOT IMPLEMENTED
   */
  @Override
  public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
    return null;
  }

  /*
   * NOT IMPLEMENTED
   */
  @Override
  public void setPartitionFilter(Expression arg0) throws IOException {
  }
}
