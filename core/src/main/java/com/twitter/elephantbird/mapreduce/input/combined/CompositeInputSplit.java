package com.twitter.elephantbird.mapreduce.input.combined;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import com.twitter.elephantbird.util.SplitUtil;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This InputSplit wraps a number of child InputSplits. Any InputSplit inserted
 * into this collection must have a public default constructor. This is based on
 * code inHadoop, but is not available in past versions so we copy it here.
 * Also, the version in Hadoop had some bugs.
 */
public class CompositeInputSplit extends InputSplit implements Writable, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(SplitUtil.class);

  private long totalSplitSizes = 0L;
  private List<InputSplit> splits = new ArrayList<InputSplit>();
  private Configuration conf = null;
  private String[] locations;

  public CompositeInputSplit() {
    // Writable
  }

  public CompositeInputSplit(List<InputSplit> splits) throws IOException, InterruptedException {
    for (InputSplit split : splits) {
      add(split);
    }
  }

  /**
   * Add an InputSplit to this collection.
   * @throws IOException If capacity was not specified during construction
   *                     or if capacity has been reached.
   */
  public void add(InputSplit split) throws IOException, InterruptedException {
    splits.add(split);
    totalSplitSizes += split.getLength();
    locations = null;
  }

  public List<InputSplit> getSplits() {
    return splits;
  }

  /**
   * Get ith child InputSplit.
   */
  public InputSplit get(int i) {
    return splits.get(i);
  }

  /**
   * Return the aggregate length of all child InputSplits currently added.
   */
  public long getLength() {
    return totalSplitSizes;
  }

  /**
   * Get the length of ith child InputSplit.
   */
  public long getLength(int i) throws IOException, InterruptedException {
    return splits.get(i).getLength();
  }

  /**
   * Collect a set of hosts from all child InputSplits.
   */
  public String[] getLocations() throws IOException, InterruptedException {
    if (locations == null) {
      Set<String> hosts = new HashSet<String>();
      for (InputSplit s : splits) {
        String[] hints = s.getLocations();
        if (hints != null) {
          for (String host : hints) {
            hosts.add(host);
          }
        }
      }
      locations = hosts.toArray(new String[hosts.size()]);
    }
    return locations;
  }

  /**
   * getLocations from ith InputSplit.
   */
  public String[] getLocation(int i) throws IOException, InterruptedException {
    return splits.get(i).getLocations();
  }

  /**
   * Write splits in the following format.
   * {@code
   * <count><class1><split1><class2><split2>...<classn><splitn>
   * }
   */
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    if (conf == null) {
      throw new IOException("Hadoop Configuration not set via setConf");
    }
    WritableUtils.writeVInt(out, splits.size());
    for (InputSplit split : splits) {
      Class<? extends InputSplit> clazz = split.getClass();
      Text.writeString(out, clazz.getName());
      SerializationFactory factory = new SerializationFactory(conf);
      Serializer serializer = factory.getSerializer(clazz);
      serializer.open((DataOutputStream)out);
      serializer.serialize(split);
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException If the child InputSplit cannot be read, typically
   *                     for failing access checks.
   */
  @SuppressWarnings("unchecked")  // Generic array assignment
  public void readFields(DataInput in) throws IOException {
    if (conf == null) {
      throw new IOException("Hadoop Configuration not set via setConf");
    }
    int card = WritableUtils.readVInt(in);
    splits = new ArrayList<InputSplit>();
    try {
      for (int i = 0; i < card; ++i) {
        Class<? extends InputSplit> clazz = (Class<? extends InputSplit>) conf.getClassByName(Text.readString(in));
        InputSplit split = ReflectionUtils.newInstance(clazz, conf);
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer deserializer = factory.getDeserializer(clazz);
        deserializer.open((DataInputStream) in);
        splits.add((InputSplit) deserializer.deserialize(split));
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed split init", e);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}