package com.twitter.elephantbird.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;
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
   * but most interfaces use super classes, though the actual object is a
   * subclass (e.g. Mapper.Context).
   * <br>
   * This utility method checks the type and returns the appropriate counter.
   * In the rare (may be unexpected) case where ctx is not a
   * TaskInputOutputContext, a dummy counter is returned after printing
   * a warning.
   */
  public static Counter getCounter(JobContext ctx, String group, String counter) {
    if (ctx instanceof TaskInputOutputContext<?, ?, ?, ?>) {
      Counter c = ((TaskInputOutputContext<?, ?, ?, ?>)ctx).getCounter(group, counter);
      if (c != null) {
        return c;
      }
    }
    String name = group + ":" + counter;
    LOG.warn("Using a dummy counter for " + name + " because it does not already exist.");
    return new Counter(name, name) {};
  }

  /**
   * @deprecated use {@link #setClassConf(Configuration, String, Class)}
   */
  @Deprecated
  public static void setInputFormatClass(Configuration  conf,
                                         String         configKey,
                                         Class<?>       clazz) {
    setClassConf(conf, configKey, clazz);
  }

  /**
   * A helper to set configuration to class name.
   * Throws a RuntimeExcpetion if the
   * configuration is already set to a different class name.
   */
  public static void setClassConf(Configuration  conf,
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

  /**
   * Writes an object into a configuration by converting it to a base64 encoded string
   * obj must be Serializable
   *
   * @param key for the configuration
   * @param obj to write
   * @param conf to write to
   * @throws IOException
   */
  public static void writeObjectToConfig(String key, Object obj, Configuration conf) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    conf.set(key, Base64.encodeBase64String(baos.toByteArray()));
  }

  /**
   * Reads an object (that was written using
   * {@link #writeObjectToConfig(String, Object, Configuration)}) from a configuration
   *
   * @param key for the configuration
   * @param conf to read from
   * @return the read object
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T> T readObjectFromConfig(String key, Configuration conf) throws IOException {
    String b64 = conf.get(key);
    byte[] bytes = Base64.decodeBase64(b64);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    try {
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      LOG.error("Could not read object from config", e);
      throw new IOException(e);
    } catch (ClassCastException e) {
      LOG.error("Couldn't cast object read from config", e);
      throw new IOException(e);
    }
  }
}
