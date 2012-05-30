package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * A wrapper LoadFunc that delegates all the functionality to another loader.
 * Similar to a FilterInputStream.
 */
public class FilterLoadFunc extends LoadFunc implements LoadMetadata, LoadPushDown {

  protected LoadFunc loader;

  /**
   * @param loader This could be null. It may not be feasible to set
   *        loader during construction. It can be set later with setLoader()
   */
  public FilterLoadFunc(LoadFunc loader) {
    this.loader = loader;
  }

  public void setLoader(LoadFunc loader) {
    this.loader = loader;
  }

  // just for readability
  private boolean isSet() {
    return loader != null;
  }
  // LoadFunc implementation:

  @Override @SuppressWarnings("unchecked")
  public InputFormat getInputFormat() throws IOException {
    return isSet() ? loader.getInputFormat() : null;
  }

  @Override
  public LoadCaster getLoadCaster() throws IOException {
    return isSet() ? loader.getLoadCaster() : null;
  }

  @Override
  public Tuple getNext() throws IOException {
    return isSet() ? loader.getNext() : null;
  }

  @Override @SuppressWarnings("unchecked")
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    if (isSet()) {
      loader.prepareToRead(reader, split);
    }
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir)
      throws IOException {
    return isSet() ?
        loader.relativeToAbsolutePath(location, curDir):
        super.relativeToAbsolutePath(location, curDir);
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    if (isSet()) {
      loader.setLocation(location, job);
    }
  }

  @Override
  public void setUDFContextSignature(String signature) {
    if (isSet()) {
      loader.setUDFContextSignature(signature);
    } else {
      super.setUDFContextSignature(signature);
    }
  }

  // LoadMetadata & LoadPushDown interface.

  // helpers for casting:
  private static LoadMetadata asLoadMetadata(LoadFunc loader) {
    return loader instanceof LoadMetadata  ? (LoadMetadata) loader : null;
  }

  private static LoadPushDown asLoadPushDown(LoadFunc loader) {
    return loader instanceof LoadPushDown  ? (LoadPushDown) loader : null;
  }


  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    LoadMetadata metadata = asLoadMetadata(loader);
    return metadata == null ? null :  metadata.getPartitionKeys(location, job);
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    LoadMetadata metadata = asLoadMetadata(loader);
    return metadata == null ? null : metadata.getSchema(location, job);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    LoadMetadata metadata = asLoadMetadata(loader);
    return metadata == null ? null : metadata.getStatistics(location, job);
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    LoadMetadata metadata = asLoadMetadata(loader);
    if ( metadata != null ) {
      metadata.setPartitionFilter(partitionFilter);
    }
  }

  @Override
  public List<OperatorSet> getFeatures() {
    LoadPushDown pushDown = asLoadPushDown(loader);
    return pushDown == null ? null : pushDown.getFeatures();
  }

  @Override
  public RequiredFieldResponse pushProjection(
      RequiredFieldList requiredFieldList) throws FrontendException {
    LoadPushDown pushDown = asLoadPushDown( loader );
    return pushDown == null ? null : pushDown.pushProjection( requiredFieldList );
  }

}
