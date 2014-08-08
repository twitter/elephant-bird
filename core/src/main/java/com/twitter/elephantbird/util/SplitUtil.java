package com.twitter.elephantbird.util;

import com.twitter.elephantbird.mapreduce.input.combine.CompositeInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This code facilitates combining InputSplits, managing the locality vs
 * input size skew tradeoff. This code is adapted and cleaned up from
 * Apache Pig. It was copied to avoid having to depend on all of Pig for
 * this utility.
 *
 * @author Jonathan Coveney
 */
public class SplitUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SplitUtil.class);

  public static final  String COMBINE_SPLIT_SIZE = "elephantbird.combine.split.size";

  private static long getCombinedSplitSize(Configuration conf) throws IOException {
    long splitSize = conf.getLong(COMBINE_SPLIT_SIZE, -1);
    if (splitSize == -1) {
      splitSize = FileSystem.get(conf).getDefaultBlockSize(new Path("."));
    }
    return splitSize;
  }

  private static class Node {
    private long length = 0;
    private List<ComparableSplit> splits;
    private boolean sorted;

    public Node() throws IOException, InterruptedException {
      length = 0;
      splits = new ArrayList<ComparableSplit>();
      sorted = false;
    }

    public void add(ComparableSplit split) throws IOException, InterruptedException {
      splits.add(split);
      length++;
    }

    public void remove(ComparableSplit split) {
      if (!sorted) {
        sort();
      }
      int index = Collections.binarySearch(splits, split);
      if (index >= 0) {
        splits.remove(index);
        length--;
      }
    }

    public void sort() {
      if (!sorted) {
        Collections.sort(splits);
        sorted = true;
      }
    }

    public List<ComparableSplit> getSplits() {
      return splits;
    }

    public long getLength() {
      return length;
    }
  }

  private static Comparator<Node> nodeComparator = new Comparator<Node>() {
    @Override
    public int compare(Node o1, Node o2) {
      return Long.signum(o1.length - o2.length);
    }
  };

  private static final class ComparableSplit implements Comparable<ComparableSplit> {
    private InputSplit rawInputSplit;
    private Set<Node> nodes;
    // id used as a tie-breaker when two splits are of equal size.
    private long id;
    public ComparableSplit(InputSplit split, long id) {
      rawInputSplit = split;
      nodes = new HashSet<Node>();
      this.id = id;
    }

    public void add(Node node) {
      nodes.add(node);
    }

    public void removeFromNodes() {
      for (Node node : nodes) {
        node.remove(this);
      }
    }

    public InputSplit getSplit() {
      return rawInputSplit;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof ComparableSplit))
        return false;
      return (compareTo((ComparableSplit) other) == 0);
    }

    @Override
    public int hashCode() {
      int hashCode = 31 + rawInputSplit.hashCode();
      for (Node node : nodes) {
        hashCode = 17 * hashCode + node.hashCode();
      }
      return hashCode;
    }

    @Override
    public int compareTo(ComparableSplit other) {
      try {
        int cmp = -Long.signum(rawInputSplit.getLength() - other.rawInputSplit.getLength());
        // in descending order
        return cmp == 0 ? Long.signum(id - other.id) : cmp;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class DummySplit extends InputSplit {
    private long length;

    @Override
    public String[] getLocations() {
      return null;
    }

    @Override
    public long getLength() {
      return length;
    }

    public void setLength(long length) {
      this.length = length;
    }
  }

  private static void removeSplits(List<ComparableSplit> splits) {
    for (ComparableSplit split: splits) {
      split.removeFromNodes();
    }
  }

  public static List<List<InputSplit>> getCombinedSplits(
          List<InputSplit> oneInputSplits, long maxCombinedSplitSize, Configuration conf)
          throws IOException, InterruptedException {
    List<Node> nodes = new ArrayList<Node>();
    Map<String, Node> nodeMap = new HashMap<String, Node>();
    List<List<InputSplit>> result = new ArrayList<List<InputSplit>>();
    List<Long> resultLengths = new ArrayList<Long>();
    long comparableSplitId = 0;

    int size = 0, nSplits = oneInputSplits.size();
    InputSplit lastSplit = null;
    int emptyCnt = 0;
    for (InputSplit split : oneInputSplits) {
      if (split.getLength() == 0) {
        emptyCnt++;
        continue;
      }
      if (split.getLength() >= maxCombinedSplitSize) {
        comparableSplitId++;
        List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
        combinedSplits.add(split);
        result.add(combinedSplits);
        resultLengths.add(split.getLength());
      } else {
        ComparableSplit csplit = new ComparableSplit(split, comparableSplitId++);
        String[] locations = split.getLocations();
        // sort the locations to stabilize the number of maps: PIG-1757
        Arrays.sort(locations);
        Set<String> locationSeen = new HashSet<String>();
        for (String location : locations) {
          if (!locationSeen.contains(location)) {
            Node node = nodeMap.get(location);
            if (node == null) {
              node = new Node();
              nodes.add(node);
              nodeMap.put(location, node);
            }
            node.add(csplit);
            csplit.add(node);
            locationSeen.add(location);
          }
        }
        lastSplit = split;
        size++;
      }
    }

    if (nSplits > 0 && emptyCnt == nSplits) {
      // if all splits are empty, add a single empty split as currently an empty directory is
      // not properly handled somewhere
      List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
      combinedSplits.add(oneInputSplits.get(0));
      result.add(combinedSplits);
    } else if (size == 1) {
      List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
      combinedSplits.add(lastSplit);
      result.add(combinedSplits);
    } else if (size > 1) {
      // combine small splits
      Collections.sort(nodes, nodeComparator);
      DummySplit dummy = new DummySplit();
      // dummy is used to search for next split of suitable size to be combine
      ComparableSplit dummyComparableSplit = new ComparableSplit(dummy, -1);
      for (Node node : nodes) {
        // sort the splits on this node in descending order
        node.sort();
        long totalSize = 0;
        List<ComparableSplit> splits = node.getSplits();
        int idx;
        int lenSplits;
        List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
        List<ComparableSplit> combinedComparableSplits = new ArrayList<ComparableSplit>();
        while (!splits.isEmpty()) {
          combinedSplits.add(splits.get(0).getSplit());
          combinedComparableSplits.add(splits.get(0));
          int startIdx = 1;
          lenSplits = splits.size();
          totalSize += splits.get(0).getSplit().getLength();
          long spaceLeft = maxCombinedSplitSize - totalSize;
          dummy.setLength(spaceLeft);
          idx = Collections.binarySearch(node.getSplits().subList(startIdx, lenSplits), dummyComparableSplit);
          idx = -idx-1+startIdx;
          while (idx < lenSplits) {
            long thisLen = splits.get(idx).getSplit().getLength();
            combinedSplits.add(splits.get(idx).getSplit());
            combinedComparableSplits.add(splits.get(idx));
            totalSize += thisLen;
            spaceLeft -= thisLen;
            if (spaceLeft <= 0)
              break;
            // find next combinable chunk
            startIdx = idx + 1;
            if (startIdx >= lenSplits)
              break;
            dummy.setLength(spaceLeft);
            idx = Collections.binarySearch(node.getSplits().subList(startIdx, lenSplits), dummyComparableSplit);
            idx = -idx-1+startIdx;
          }
          if (totalSize > maxCombinedSplitSize/2) {
            result.add(combinedSplits);
            resultLengths.add(totalSize);
            removeSplits(combinedComparableSplits);
            totalSize = 0;
            combinedSplits = new ArrayList<InputSplit>();
            combinedComparableSplits.clear();
            splits = node.getSplits();
          } else {
            if (combinedSplits.size() != lenSplits)
              throw new AssertionError("Combined split logic error!");
            break;
          }
        }
      }
      // handle leftovers
      List<ComparableSplit> leftoverSplits = new ArrayList<ComparableSplit>();
      Set<InputSplit> seen = new HashSet<InputSplit>();
      for (Node node : nodes) {
        for (ComparableSplit split : node.getSplits()) {
          if (!seen.contains(split.getSplit())) {
            // remove duplicates. The set has to be on the raw input split not the
            // comparable input split as the latter overrides the compareTo method
            // so its equality semantics is changed and not we want here
            seen.add(split.getSplit());
            leftoverSplits.add(split);
          }
        }
      }

      if (!leftoverSplits.isEmpty()) {
        long totalSize = 0;
        List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
        List<ComparableSplit> combinedComparableSplits = new ArrayList<ComparableSplit>();

        int splitLen = leftoverSplits.size();
        for (int i = 0; i < splitLen; i++) {
          ComparableSplit split = leftoverSplits.get(i);
          long thisLen = split.getSplit().getLength();
          if (totalSize + thisLen >= maxCombinedSplitSize) {
            removeSplits(combinedComparableSplits);
            result.add(combinedSplits);
            resultLengths.add(totalSize);
            combinedSplits = new ArrayList<InputSplit>();
            combinedComparableSplits.clear();
            totalSize = 0;
          }
          combinedSplits.add(split.getSplit());
          combinedComparableSplits.add(split);
          totalSize += split.getSplit().getLength();
          if (i == splitLen - 1) {
            // last piece: it could be very small, try to see it can be squeezed into any existing splits
            for (int j =0; j < result.size(); j++) {
              if (resultLengths.get(j) + totalSize <= maxCombinedSplitSize) {
                List<InputSplit> isList = result.get(j);
                for (InputSplit csplit : combinedSplits) {
                  isList.add(csplit);
                }
                removeSplits(combinedComparableSplits);
                combinedSplits.clear();
                break;
              }
            }
            if (!combinedSplits.isEmpty()) {
              // last piece can not be squeezed in, create a new combine split for them.
              removeSplits(combinedComparableSplits);
              result.add(combinedSplits);
            }
          }
        }
      }
    }
    LOG.info("Original input paths (" + oneInputSplits.size() + ") combine into (" + result.size() + ")");
    return result;
  }

  public static List<CompositeInputSplit> getCombinedCompositeSplits(
          List<InputSplit> oneInputSplits, long maxCombinedSplitSize, Configuration conf)
          throws IOException, InterruptedException {
    List<CompositeInputSplit> compositeInputSplits = new ArrayList<CompositeInputSplit>(oneInputSplits.size());
    for (List<InputSplit> inputSplits : getCombinedSplits(oneInputSplits, maxCombinedSplitSize, conf)) {
      compositeInputSplits.add(new CompositeInputSplit(inputSplits));
    }
    return compositeInputSplits;
  }

  public static List<CompositeInputSplit> getCombinedCompositeSplits(
          List<InputSplit> oneInputSplits, Configuration conf)
          throws IOException, InterruptedException {
    return getCombinedCompositeSplits(oneInputSplits, getCombinedSplitSize(conf), conf);
  }

  public static void serializeInputSplit(Configuration conf, DataOutputStream out, InputSplit split)
      throws IOException {
    Class<? extends InputSplit> clazz = split.getClass().asSubclass(InputSplit.class);
    Text.writeString(out, clazz.getName());
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer serializer = factory.getSerializer(clazz);
    serializer.open(out instanceof UncloseableDataOutputStream ? out : new UncloseableDataOutputStream(out));
    serializer.serialize(split);
  }

  public static InputSplit deserializeInputSplit(Configuration conf, DataInputStream in) throws IOException {
    String name = Text.readString(in);
    Class<? extends InputSplit> clazz;
    try {
      clazz = conf.getClassByName(name).asSubclass(InputSplit.class);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not find class for deserialized class name: " + name, e);
    }
    return deserializeInputSplitInternal(
        conf, in instanceof UncloseableDataInputStream ? in : new UncloseableDataInputStream(in), clazz);
  }

  private static <T extends InputSplit> T deserializeInputSplitInternal(
      Configuration conf, DataInputStream in, Class<T> clazz) throws IOException {
    T split = ReflectionUtils.newInstance(clazz, conf);
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer<T> deserializer = factory.getDeserializer(clazz);
    deserializer.open(in instanceof UncloseableDataInputStream ? in : new UncloseableDataInputStream(in));
    return deserializer.deserialize(split);
  }

  private static class UncloseableDataOutputStream extends DataOutputStream {
    public UncloseableDataOutputStream(DataOutputStream os) {
      super(os);
    }

    @Override
    public void close() {
      // We don't want classes given this stream to close it
    }
  }

  private static class UncloseableDataInputStream extends DataInputStream {
    public UncloseableDataInputStream(DataInputStream is) {
      super(is);
    }

    @Override
    public void close() {
      // We don't want classes given this stream to close it
    }
  }

}
