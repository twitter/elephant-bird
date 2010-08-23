package com.twitter.elephantbird.pig.util;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigHadoopLogger;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class to deal with Hadoop counters in Pig.  They are stored within the singleton
 * PigHadoopLogger instance, but are null for some period of time at job startup, even after
 * Pig has been invoked.  This class buffers counters, trying each time to get a valid Reporter and flushing
 * stored counters each time it does.
 */
public class PigCounterHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PigCounterHelper.class);

  private Map<Pair<String, String>, Long> counterStringMap_ = Maps.newHashMap();
  private Map<Enum<?>, Long> counterEnumMap_ = Maps.newHashMap();
  private TaskInputOutputContext tioc;

  /**
   * Mocks the Reporter.incrCounter, but adds buffering.
   * See org.apache.hadoop.mapred.Reporter's incrCounter.
   */
  public void incrCounter(String group, String counter, long incr) {
    Pair<String, String> key = new Pair<String, String>(group, counter);
    Long currentValue = counterStringMap_.get(key);
    counterStringMap_.put(key, (currentValue == null ? 0 : currentValue) + incr);

    if (getTIOC() != null) {
      for (Map.Entry<Pair<String, String>, Long> entry : counterStringMap_.entrySet()) {
    	  tioc.getCounter(entry.getKey().first, entry.getKey().second).increment(entry.getValue());
      }
      counterStringMap_.clear();
    }
  }

  /**
   * Mocks the Reporter.incrCounter, but adds buffering.
   * See org.apache.hadoop.mapred.Reporter's incrCounter.
   */
  public void incrCounter(Enum<?> key, long incr) {
    Long currentValue = counterEnumMap_.get(key);
    counterEnumMap_.put(key, (currentValue == null ? 0 : currentValue) + incr);

    if (getTIOC() != null) {
      for (Map.Entry<Enum<?>, Long> entry : counterEnumMap_.entrySet()) {
        getTIOC().getCounter(entry.getKey()).increment(entry.getValue());
      }
      counterEnumMap_.clear();
    }
  }

  /**
   * Try for the Reporter object if it hasn't been initialized yet, otherwise just return it.
   * @return the job's reporter object, or null if it isn't retrievable yet.
   */
  private TaskInputOutputContext getTIOC() {
    if (tioc == null) {
    	tioc = PigHadoopLogger.getInstance().getTaskIOContext();
    }
    return tioc;
  }
}
