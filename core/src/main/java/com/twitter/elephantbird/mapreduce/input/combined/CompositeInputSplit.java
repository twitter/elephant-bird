package com.twitter.elephantbird.mapreduce.input.combined;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

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
public class CompositeInputSplit extends InputSplit implements Writable {
  private long totalSplitSizes = 0L;
  private List<InputSplit> splits = new ArrayList<InputSplit>();
  private Configuration conf = new Configuration();

  public CompositeInputSplit() {
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
    HashSet<String> hosts = new HashSet<String>();
    for (InputSplit s : splits) {
      String[] hints = s.getLocations();
      if (hints != null && hints.length > 0) {
        for (String host : hints) {
          hosts.add(host);
        }
      }
    }
    return hosts.toArray(new String[hosts.size()]);
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
   * <count><class1><class2>...<classn><split1><split2>...<splitn>
   * }
   */
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, splits.size());
    for (InputSplit s : splits) {
      Text.writeString(out, s.getClass().getName());
    }
    for (InputSplit s : splits) {
      SerializationFactory factory = new SerializationFactory(conf);
      Serializer serializer =
              factory.getSerializer(s.getClass());
      serializer.open((DataOutputStream)out);
      serializer.serialize(s);
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException If the child InputSplit cannot be read, typically
   *                     for failing access checks.
   */
  @SuppressWarnings("unchecked")  // Generic array assignment
  public void readFields(DataInput in) throws IOException {
    int card = WritableUtils.readVInt(in);
    splits = new ArrayList<InputSplit>();
    Class<? extends InputSplit>[] cls = new Class[card];
    try {
      for (int i = 0; i < card; ++i) {
        cls[i] = Class.forName(Text.readString(in)).asSubclass(InputSplit.class);
      }
      for (int i = 0; i < card; ++i) {
        InputSplit split = ReflectionUtils.newInstance(cls[i], null);
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer deserializer = factory.getDeserializer(cls[i]);
        deserializer.open((DataInputStream)in);
        splits.add((InputSplit) deserializer.deserialize(split));
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed split init", e);
    }
  }
}