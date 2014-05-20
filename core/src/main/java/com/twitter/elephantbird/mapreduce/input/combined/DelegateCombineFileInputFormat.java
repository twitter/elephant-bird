package com.twitter.elephantbird.mapreduce.input.combined;

import com.twitter.elephantbird.util.SplitUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * This class allows for use of {@link CombineFileInputFormat} with Elephant Bird's other
 * input formats. It works seamlessly with
 * {@link com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper} which
 * means that any of Elephant Bird's Input Formats can be used in Cascading
 * as a CombineFileInputFormat.
 *
 * @author Jonathan Coveney
 */
public class DelegateCombineFileInputFormat<K, V> extends CombineFileInputFormat<K, V> {
  public static final String USE_COMBINED_INPUT_FORMAT = "elephantbird.use.combined.input.format";

  public static void setSplitMinSizePerNode(Configuration conf, long value) {
    conf.setLong(SPLIT_MINSIZE_PERNODE, value);
  }

  public static void setSplitMinSizePerRack(Configuration conf, long value) {
    conf.setLong(SPLIT_MINSIZE_PERRACK, value);
  }

  private InputFormat<K, V> delegate;
  private long maxSplitSize;
  private long minSplitSizeNode;
  private long minSplitSizeRack;

  // A pool of input paths filters. A split cannot have blocks from files
  // across multiple pools.
  private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>(); //TODO how do we want to incorporate this!

  //TODO theoretically we could also have this take a class reference and a configuration, so then
  // it could arbitrarily compose (since an InputFormat needs this anyway)
  public DelegateCombineFileInputFormat(InputFormat<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public RecordReader createRecordReader(
          InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    return new CompositeRecordReader(delegate);
  }

  @Override
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  @Override
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  @Override
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  @Override
  protected void createPool(List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  @Override
  protected void createPool(PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f: filters) {
      multi.add(f);
    }
    pools.add(multi);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> inputSplits;
    try {
      inputSplits = delegate.getSplits(job);
    } catch (InterruptedException e) {
      throw new IOException("Delegate error on getSplits", e);
    }
    List<InputSplit> combinedInputSplits = new ArrayList<InputSplit>();
    Configuration conf = job.getConfiguration();
    try {
      for (CompositeInputSplit split : SplitUtil.getCombinedCompositeSplits(inputSplits, conf)) {
        split.setConf(conf);
        combinedInputSplits.add(split);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return combinedInputSplits;
  }

  /**
   * Accept a path only if any one of filters given in the
   * constructor do. This is taken from {@link CombineFileInputFormat}
   * as it does not make it public.
   */
  public static final class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter() {
      this.filters = new ArrayList<PathFilter>();
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("[");
      for (PathFilter f: filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }
}
