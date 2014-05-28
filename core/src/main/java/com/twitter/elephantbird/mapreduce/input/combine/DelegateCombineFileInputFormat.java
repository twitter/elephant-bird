package com.twitter.elephantbird.mapreduce.input.combine;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.SplitUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * This class allows for combining the InputSplit of an underlying {@link InputFormat}
 * in a way which functions properly with Elephant Bird's other input formats. It works
 * seamlessly with {@link com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper}
 * which means that any of Elephant Bird's Input Formats can be used in Cascading
 * as a CombineFileInputFormat.
 *
 * @author Jonathan Coveney
 */
public class DelegateCombineFileInputFormat<K, V> extends FileInputFormat<K, V> {
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

  private InputFormat<K, V> delegate;
  private long maxSplitSize;
  private long minSplitSizeNode;
  private long minSplitSizeRack;

  public static void setDelegateInputFormat(JobConf conf, Class<? extends InputFormat> inputFormat) {
    DeprecatedInputFormatWrapper.setInputFormat(DelegateCombineFileInputFormat.class, conf);
    setCombinedInputFormatDelegate(conf, inputFormat);
  }

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

  private boolean shouldCombine(Configuration conf) {
    return conf.getBoolean(USE_COMBINED_INPUT_FORMAT, false);
  }

  @Override
  public RecordReader createRecordReader(
          InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration conf = HadoopCompat.getConfiguration(taskAttemptContext);
    initInputFormat(conf);
    if (shouldCombine(conf)) {
      return new CompositeRecordReader(delegate);
    } else {
      return delegate.createRecordReader(inputSplit, taskAttemptContext);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = HadoopCompat.getConfiguration(job);
    initInputFormat(conf);
    try {
      if (shouldCombine(conf)) {
          List<InputSplit> inputSplits = delegate.getSplits(job);
          List<InputSplit> combinedInputSplits = new ArrayList<InputSplit>();
          for (CompositeInputSplit split : SplitUtil.getCombinedCompositeSplits(inputSplits, conf)) {
            split.setConf(conf);
            combinedInputSplits.add(split);
          }
          return combinedInputSplits;
      } else {
        return delegate.getSplits(job);
      }
    } catch (InterruptedException e) {
      LOG.error("Thread interrupted", e);
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }
}
