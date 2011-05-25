package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * This class handles LZO-decoding and slicing input LZO files.  It expects the
 * filenames to end in .lzo, otherwise it assumes they are not compressed and skips them.
 * TODO: Improve the logic to accept a mixture of lzo and non-lzo files.
 */
public abstract class LzoBaseLoadFunc extends LoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseLoadFunc.class);

  protected final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();

  protected RecordReader reader_;

  // The load func spec is the load function name (with classpath) plus the arguments.
  protected FuncSpec loadFuncSpec_;

  // Making accessing Hadoop counters from Pig slightly more convenient.
  private final PigCounterHelper counterHelper_ = new PigCounterHelper();

  protected Configuration jobConf;

  /**
   * Construct a new load func.
   */
  public LzoBaseLoadFunc() {
    // By default, the spec is the class being loaded with no arguments.
    setLoaderSpec(getClass(), new String[] {});
  }

  /**
   * Set the loader spec so any arguments given in the script are tracked, to be reinstantiated by the mappers.
   * @param clazz the class of the load function to use.
   * @param args an array of strings that are fed to the class's constructor.
   */
  protected void setLoaderSpec(Class <? extends LzoBaseLoadFunc> clazz, String[] args) {
    loadFuncSpec_ = new FuncSpec(clazz.getName(), args);
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

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
      this.reader_ = reader;
  }

}
