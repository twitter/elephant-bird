package com.twitter.elephantbird.pig.util;

import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.google.common.collect.Maps;

/**
 * A helper class to deal with Hadoop counters in Pig.  They are stored within the singleton
 * PigStatusReporter instance, but are null for some period of time at job startup, even after
 * Pig has been invoked.  This class buffers counters, trying each time to get a valid Reporter and flushing
 * stored counters each time it does.
 */
public class PigCounterHelper {
  private final Map<Pair<String, String>, Long> counterStringMap_ = Maps.newHashMap();
  private final Map<Enum<?>, Long> counterEnumMap_ = Maps.newHashMap();

  /**
   * Mocks the Reporter.incrCounter, but adds buffering.
   * See org.apache.hadoop.mapred.Reporter's incrCounter.
   */
  public void incrCounter(String group, String counterName, long incr) {
    PigStatusReporter reporter = PigStatusReporter.getInstance();
    if (reporter != null) { // common case
      Counter counter = reporter.getCounter(group, counterName);
      if (counter != null) {
        counter.increment(incr);

        if (counterStringMap_.size() > 0) {
          for (Map.Entry<Pair<String, String>, Long> entry : counterStringMap_.entrySet()) {
            reporter.getCounter(entry.getKey().first, entry.getKey().second).increment(entry.getValue());
          }
          counterStringMap_.clear();
        }
        return;
      }
    }
    // In the case when reporter is not available, or we can't get the Counter,
    // store in the local map.
    Pair<String, String> key = new Pair<String, String>(group, counterName);
    Long currentValue = counterStringMap_.get(key);
    counterStringMap_.put(key, (currentValue == null ? 0 : currentValue) + incr);
  }

  /**
   * Mocks the Reporter.incrCounter, but adds buffering.
   * See org.apache.hadoop.mapred.Reporter's incrCounter.
   */
  public void incrCounter(Enum<?> key, long incr) {
    PigStatusReporter reporter = PigStatusReporter.getInstance();
    if (reporter != null && reporter.getCounter(key) != null) {
      reporter.getCounter(key).increment(incr);
      if (counterEnumMap_.size() > 0) {
        for (Map.Entry<Enum<?>, Long> entry : counterEnumMap_.entrySet()) {
          reporter.getCounter(entry.getKey()).increment(entry.getValue());
        }
        counterEnumMap_.clear();
      }
    } else { // buffer the increments
      Long currentValue = counterEnumMap_.get(key);
      counterEnumMap_.put(key, (currentValue == null ? 0 : currentValue) + incr);
    }
  }
}
