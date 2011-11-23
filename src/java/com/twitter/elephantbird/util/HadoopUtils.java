package com.twitter.elephantbird.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various Hadoop specific utilities.
 */
public class HadoopUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

  /**
   * MapReduce counters are available only with {@link TaskInputOutputContext},
   * but most interfaces use super classes, though the actual obejct is a 
   * subclass (e.g. Mapper.Context). <br> <br>
   * 
   * This utility method checks the type and returns the appropriate counter.
   * In the rare (may be unexpected) case where ctx is not a 
   * TaskInputOutputContext, a dummy counter is returned after printing
   * a warning.
   */
  public static Counter getCounter(JobContext ctx, String group, String counter) {
    if (ctx instanceof TaskInputOutputContext<?, ?, ?, ?>) {
      return ((TaskInputOutputContext<?, ?, ?, ?>)ctx).getCounter(group, counter);
    }
    String name = group + ":" + counter;
    LOG.warn("Context is not a TaskInputOutputContext. "
        + "will return a dummy counter for '" + name + "'");
    return new Counter(name, name) {};
  }

  /**
   * A helper to set configuration to class name.
   * Throws a RuntimeExcpetion if the
   * configuration is already set to a different class name.
   */
  public static void setInputFormatClass(Configuration  conf,
                                         String         configKey,
                                         Class<?>       clazz) {
    String existingClass = conf.get(configKey);
    String className = clazz.getName();

    if (existingClass != null && !existingClass.equals(className)) {
        throw new RuntimeException(
            "Already registered a different thriftClass for "
            + configKey
            + ". old: " + existingClass
            + " new: " + className);
    } else {
      conf.set(configKey, className);
    }
  }
}
