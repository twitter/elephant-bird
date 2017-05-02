package com.twitter.elephantbird.mapreduce.input.combine;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import com.twitter.elephantbird.util.Pair;
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
 * code in Hadoop, but is not available in past versions so we copy it here.
 * Also, the version in Hadoop had some bugs.
 */
public class CompositeInputSplit extends InputSplit implements Writable, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeInputSplit.class);

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
   * Collect a set of hosts from all child InputSplits. Note that this is just a hint to the MapReduce framework
   * for where the task should go, so we return the top 5, as returning too many can be expensive on the MR side
   * of things.
   */
  public String[] getLocations() throws IOException, InterruptedException {
    if (locations == null) {
      Map<String, Integer> hosts = new HashMap<String, Integer>();
      for (InputSplit s : splits) {
        String[] hints = s.getLocations();
        if (hints != null) {
          for (String host : hints) {
            Integer value = hosts.get(host);
            if (value == null) {
              value = 0;
            }
            value++;
            hosts.put(host, value);
          }
        }
      }
      if (hosts.size() < 5) {
        locations = hosts.keySet().toArray(new String[hosts.size()]);
      } else {
        Queue<Pair<String, Integer>> queue = new PriorityQueue<Pair<String, Integer>>(hosts.size(), new Comparator<Pair<String, Integer>>() {
          public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
            return -o1.getSecond().compareTo(o2.getSecond());
          }
        });
        for (Map.Entry<String, Integer> entry : hosts.entrySet()) {
          queue.add(new Pair<String, Integer>(entry.getKey(), entry.getValue()));
        }
        locations = new String[] {
          queue.remove().getFirst(),
          queue.remove().getFirst(),
          queue.remove().getFirst(),
          queue.remove().getFirst(),
          queue.remove().getFirst()
        };
      }
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
      SplitUtil.serializeInputSplit(conf, (DataOutputStream) out, split);
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
    for (int i = 0; i < card; ++i) {
      splits.add(SplitUtil.deserializeInputSplit(conf, (DataInputStream) in));
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

  @Override
  public String toString() {
    return "CompositeInputSplit(totalSplitSizes=" + totalSplitSizes + ", splits=" + splits + ")";
  }
}