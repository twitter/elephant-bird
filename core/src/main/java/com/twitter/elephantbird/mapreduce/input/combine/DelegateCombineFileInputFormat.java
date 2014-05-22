package com.twitter.elephantbird.mapreduce.input.combine;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatValueCopier;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.SplitUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(DelegateCombineFileInputFormat.class);

  public static final String USE_COMBINED_INPUT_FORMAT = "elephantbird.use.combine.input.format";
  public static final String COMBINED_INPUT_FORMAT_DELEGATE = "elephantbird.combine.input.format.delegate";

  public static void setUseCombinedInputFormat(Configuration conf) {
    conf.setBoolean(USE_COMBINED_INPUT_FORMAT, true);
  }

  // This sets configures the delegate, though it does not configure DelegateCombineFileInputFormat.
  public static void setCombinedInputFormatDelegate(Configuration conf, Class<? extends InputFormat> clazz) {
    HadoopUtils.setClassConf(conf, COMBINED_INPUT_FORMAT_DELEGATE, clazz);
  }

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

  private void initInputFormat(Configuration conf) throws IOException {
    if (delegate == null) {
      Class<? extends InputFormat> delegateClass =
        conf.getClass(COMBINED_INPUT_FORMAT_DELEGATE, null, InputFormat.class);
      if (delegateClass == null) {
        throw new IOException("No delegate class was set on key: " + COMBINED_INPUT_FORMAT_DELEGATE);
      }
      delegate = ReflectionUtils.newInstance(delegateClass, conf);
    }
  }

  public DelegateCombineFileInputFormat() {
    // Will instantiate the delegate via reflection
  }

  public DelegateCombineFileInputFormat(InputFormat<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public RecordReader createRecordReader(
          InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    initInputFormat(taskAttemptContext.getConfiguration());
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
    initInputFormat(HadoopCompat.getConfiguration(job));
    List<InputSplit> inputSplits;
    try {
      inputSplits = delegate.getSplits(job);
    } catch (InterruptedException e) {
      LOG.error("Thread interrupted", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    List<InputSplit> combinedInputSplits = new ArrayList<InputSplit>();
    Configuration conf = job.getConfiguration();
    try {
      for (CompositeInputSplit split : SplitUtil.getCombinedCompositeSplits(inputSplits, conf)) {
        split.setConf(conf);
        combinedInputSplits.add(split);
      }
    } catch (InterruptedException e) {
      LOG.error("Thread interrupted", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    return combinedInputSplits;
  }
}
