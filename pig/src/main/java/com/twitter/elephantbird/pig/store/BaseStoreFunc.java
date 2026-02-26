package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;

import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * Base class for StoreFunc implementations. Implements some of the trivial interface methods.
 */
public abstract class BaseStoreFunc extends StoreFunc {

  private final PigCounterHelper counterHelper = new PigCounterHelper();
  @SuppressWarnings("rawtypes")
  protected RecordWriter writer;
  protected String contextSignature;

  /**
   * A convenience function for working with Hadoop counter objects from load functions. The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(String group, String counter, long incr) {
    counterHelper.incrCounter(group, counter, incr);
  }

  /**
   * A convenience function for working with Hadoop counter objects from load functions. The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(Enum<?> key, long incr) {
    counterHelper.incrCounter(key, incr);
  }

  /** same as incrCounter(pair.first, pair.second, incr). */
  protected void incrCounter(Pair<String, String> groupCounterPair, long incr) {
    counterHelper.incrCounter(groupCounterPair.first, groupCounterPair.second, incr);
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }

  /** UDF properties for this class based on context signature */
  protected Properties getUDFProperties() {
    return UDFContext.getUDFContext()
        .getUDFProperties(this.getClass(), new String[] { contextSignature });
  }

  @Override
  public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  /**
   * Invokes writer.write(key, value). <br>
   * The main purpose is to catch {@link InterruptedException} and covert it to an
   * {@link IOException}. Avoids unnecessary try-catch dance in putNext() implementations.
   */
  @SuppressWarnings("unchecked")
  final protected void writeRecord(Object key, Object value) throws IOException {
    try {
      writer.write(key, value);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
