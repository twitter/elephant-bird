package com.twitter.elephantbird.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Various Hadoop specific utilities.
 */
public class HadoopUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

  private HadoopUtils() { }

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
    return new GenericCounter(name, name) {};
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
  public static void writeObjectToConfAsBase64(String key, Object obj, Configuration conf) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();

    conf.set(key, new String(Base64.encodeBase64(baos.toByteArray()), Charsets.UTF_8));
  }

  /**
   * Reads an object (that was written using
   * {@link #writeObjectToConfAsBase64}) from a configuration
   *
   * @param key for the configuration
   * @param conf to read from
   * @return the read object, or null if key is not present in conf
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T> T readObjectFromConfAsBase64(String key, Configuration conf) throws IOException {
    String b64 = conf.get(key);
    if (b64 == null) {
      return null;
    }
    byte[] bytes = Base64.decodeBase64(b64.getBytes(Charsets.UTF_8));
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    try {
      return (T) ois.readObject();
    } catch (ClassNotFoundException e) {
      LOG.error("Could not read object from config with key " + key, e);
      throw new IOException(e);
    } catch (ClassCastException e) {
      LOG.error("Couldn't cast object read from config with key " + key, e);
      throw new IOException(e);
    }
  }

  /**
   * Writes a list of strings into a configuration by converting it to a json array
   *
   * @param key for the configuration
   * @param list to write
   * @param conf to write to
   */
  public static void writeStringListToConfAsJson(String key,
                                                 List<String> list,
                                                 Configuration conf) {
    Preconditions.checkNotNull(list);
    conf.set(key, JSONArray.toJSONString(list));
  }

  /**
   * Reads a list of strings stored as a json array from  a configuration
   *
   * @param key for the configuration
   * @param conf to read from
   * @return the read list of strings, or null if key is not present in conf
   */
  @SuppressWarnings("unchecked")
  public static List<String> readStringListFromConfAsJson(String key, Configuration conf) {
    String json = conf.get(key);
    if (json == null) {
      return null;
    }
    return Lists.<String>newArrayList(((JSONArray) JSONValue.parse(json)));
  }
}
