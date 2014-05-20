package com.twitter.elephantbird.mapreduce.input.combined;

import com.twitter.elephantbird.util.SplitUtil;
import org.apache.hadoop.conf.Configuration;
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
    throw new UnsupportedOperationException("pools not yet supported");
  }

  @Override
  protected void createPool(PathFilter... filters) {
    throw new UnsupportedOperationException("pools not yet supported");
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
}
