package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This class handles LZO-decoding and slicing input LZO files.  It expects the
 * filenames to end in .lzo, otherwise it assumes they are not compressed and skips them.
 * TODO: Improve the logic to accept a mixture of lzo and non-lzo files.
 */
public abstract class LzoBaseLoadFunc extends LoadFunc implements LoadMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseLoadFunc.class);

  protected final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();

  protected RecordReader reader_;

  // Making accessing Hadoop counters from Pig slightly more convenient.
  private final PigCounterHelper counterHelper_ = new PigCounterHelper();

  protected Configuration jobConf;

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

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
    this.jobConf = job.getConfiguration();
  }

  /**
   * A utility method for loaders that read {@link BinaryWritable} values.
   */
  protected <M> M getNextBinaryValue(TypeRef<M> typeRef) throws IOException {
    //typeRef is just to help compiler resolve the type at compile time.
    try {
      if (reader_ != null && reader_.nextKeyValue()) {
        @SuppressWarnings("unchecked")
        BinaryWritable<M> writable = (BinaryWritable<M>)reader_.getCurrentValue();
        return writable.get();
      }
    } catch (InterruptedException e) {
      LOG.error("InterruptedException encountered, bailing.", e);
      throw new IOException(e);
    }

    return null;
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
      this.reader_ = reader;
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
