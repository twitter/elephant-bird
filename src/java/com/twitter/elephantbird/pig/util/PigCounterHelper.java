package com.twitter.elephantbird.pig.util;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigHadoopLogger;
import org.apache.pig.impl.util.Pair;

/**
 * A helper class to deal with Hadoop counters in Pig.  They are stored within the singleton
 * PigHadoopLogger instance, but are null for some period of time at job startup, even after
 * Pig has been invoked.  This class buffers counters, trying each time to get a valid Reporter and flushing
 * stored counters each time it does.
 */
public class PigCounterHelper {
  private Map<Pair<String, String>, Long> counterStringMap_ = Maps.newHashMap();
  private Map<Enum<?>, Long> counterEnumMap_ = Maps.newHashMap();
  private PigHadoopLogger pigLogger_ = null;

  /**
   * Mocks the Reporter.incrCounter, but adds buffering.
   * See org.apache.hadoop.mapred.Reporter's incrCounter.
   */
  public void incrCounter(String group, String counter, long incr) {
    if (getReporter() != null) { // common case
      getReporter().incrCounter(group, counter, incr);
      if (counterStringMap_.size() > 0) {
        for (Map.Entry<Pair<String, String>, Long> entry : counterStringMap_.entrySet()) {
          getReporter().incrCounter(entry.getKey().first, entry.getKey().second, entry.getValue());
        }
        counterStringMap_.clear();
      }
    } else { // buffer the increments.
      Pair<String, String> key = new Pair<String, String>(group, counter);
      Long currentValue = counterStringMap_.get(key);
      counterStringMap_.put(key, (currentValue == null ? 0 : currentValue) + incr);
    }
  }

  /**
   * Mocks the Reporter.incrCounter, but adds buffering.
   * See org.apache.hadoop.mapred.Reporter's incrCounter.
   */
  public void incrCounter(Enum<?> key, long incr) {
    if (getReporter() != null) {
      getReporter().incrCounter(key, incr);
      if (counterEnumMap_.size() > 0) {
        for (Map.Entry<Enum<?>, Long> entry : counterEnumMap_.entrySet()) {
          getReporter().incrCounter(entry.getKey(), entry.getValue());
        }
        counterEnumMap_.clear();
      }
    } else { // buffer the increments
      Long currentValue = counterEnumMap_.get(key);
      counterEnumMap_.put(key, (currentValue == null ? 0 : currentValue) + incr);
    }
  }

  /**
   * Try for the Reporter object if it hasn't been initialized yet, otherwise just return it.
   * @return the job's reporter object, or null if it isn't retrievable yet.
   */
  private Reporter getReporter() {
    if (pigLogger_ == null) {
      pigLogger_ = PigHadoopLogger.getInstance();
    }
    return pigLogger_.getReporter();
  }
}
