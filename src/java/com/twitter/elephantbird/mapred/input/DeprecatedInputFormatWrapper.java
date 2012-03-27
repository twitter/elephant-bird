package com.twitter.elephantbird.mapred.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import com.twitter.elephantbird.util.HadoopUtils;

/**
 * The wrapper enables a {@link InputFormat} written for new
 * <code>mapreduce</code> interface to be used unmodified in contexts where
 * a {@link org.apache.hadoop.mapred.InputFormat} with old <code>mapred</code>
 * interface is required. </p>
 *
 * Current restrictions on InputFormat: <ul>
 *    <li> input split should be a FileSplit
 *    <li> the record reader should reuse key and value objects
 * </ul>
 *
 * While these restrictions are satisfied by most input formats,
 * they could be removed with a couple more configuration options.
 * <p>
 *
 * Usage: <pre>
 *    // set up JobConf
 *    DeprecatedInputFormatWrapper.setInputFormat(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, jobConf);
 *    jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
 *    // ...
 * </pre>
 * @author Raghu Angadi
 *
 */
@SuppressWarnings("deprecation")
public class DeprecatedInputFormatWrapper<K, V> implements org.apache.hadoop.mapred.InputFormat<K, V> {

  private static final String CLASS_CONF_KEY = "elephantbird.class.for.DeprecatedInputFormatWrapper";

  protected InputFormat<K, V> realInputFormat;

  /**
   * Sets jobs input format to {@link DeprecatedInputFormatWrapper} and stores
   * supplied real {@link InputFormat} class name in job configuration.
   * This configuration is read on the remote tasks to instantiate actual
   * InputFormat correctly.
   */
  public static void setInputFormat(Class<?> realInputFormatClass, JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedInputFormatWrapper.class);
    HadoopUtils.setInputFormatClass(jobConf, CLASS_CONF_KEY, realInputFormatClass);
  }

  @SuppressWarnings("unchecked")
  private void initInputFormat(JobConf conf) {
    if (realInputFormat == null) {
      realInputFormat = ReflectionUtils.newInstance(
                          conf.getClass(CLASS_CONF_KEY, null, InputFormat.class),
                          conf);
    }
  }

  public DeprecatedInputFormatWrapper() {
    // real inputFormat is initialized based on conf.
  }

  public DeprecatedInputFormatWrapper(InputFormat<K, V> realInputFormat) {
    this.realInputFormat = realInputFormat;
  }

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
    initInputFormat(job);
    return new RecordReaderWrapper<K, V>(realInputFormat, split, job);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // currently only FileSplit conversion is supported.
    initInputFormat(job);

    try {
      List<org.apache.hadoop.mapreduce.InputSplit> splits =
        realInputFormat.getSplits(new JobContext(job, null));

      if (splits == null) {
        return null;
      }

      FileSplit[] resultSplits = new FileSplit[splits.size()];
      int i = 0;
      for (org.apache.hadoop.mapreduce.InputSplit split : splits) {

        // assert that this is a FileSplit. Later we could let user supply
        // a converter from mapreuduce.split to mapred.split
        if (split.getClass() != org.apache.hadoop.mapreduce.lib.input.FileSplit.class) {
          throw new IOException("only FileSplit is supported in this wrapper. "
                                + "but got " + split.getClass());
        }

        org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit =
          (org.apache.hadoop.mapreduce.lib.input.FileSplit)split;

        resultSplits[i++] = new FileSplit(fsplit.getPath(),
                                          fsplit.getStart(),
                                          fsplit.getLength(),
                                          job);
      }

      return resultSplits;

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private static class RecordReaderWrapper<K, V> implements RecordReader<K, V> {

    private org.apache.hadoop.mapreduce.RecordReader<K, V> realReader;
    private long splitLen; // for getPos()

    // expect readReader return same Key & Value objects (common case)
    // this avoids extra serialization & deserialazion of these objects
    private K keyObj = null;
    private V valueObj = null;

    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(InputFormat<K, V> newInputFormat,
                               InputSplit oldSplit,
                               JobConf oldJobConf) throws IOException {

      // create newFileSplit from old FileSplit.
      FileSplit ofs = (FileSplit) oldSplit; // FileSplit is enforced in getSplits().
      org.apache.hadoop.mapreduce.lib.input.FileSplit split =
        new org.apache.hadoop.mapreduce.lib.input.FileSplit(
                                              ofs.getPath(),
                                              ofs.getStart(),
                                              ofs.getLength(),
                                              ofs.getLocations());

      splitLen = split.getLength();

      TaskAttemptContext taskContext =
        new TaskAttemptContext(oldJobConf, TaskAttemptID.forName(oldJobConf.get("mapred.task.id")));

      try {
        realReader = newInputFormat.createRecordReader(split, taskContext);
        realReader.initialize(split, taskContext);

        // read once to gain access to key and value objects
        if (realReader.nextKeyValue()) {
          firstRecord = true;
          keyObj = realReader.getCurrentKey();
          valueObj = realReader.getCurrentValue();
        } else {
          eof = true;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    @Override
    public K createKey() {
      return keyObj;
    }

    @Override
    public V createValue() {
      return valueObj;
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return realReader.getProgress();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean next(K key, V value) throws IOException {
      if (eof) {
        return false;
      }

      if (firstRecord) { // key & value are already read.
        firstRecord = false;
        return true;
      }

      try {
        if (realReader.nextKeyValue()) {

          if (key != realReader.getCurrentKey() ||
              value != realReader.getCurrentValue()) {

            throw new IOException("DeprecatedInputFormatWrapper can not "
                + "support RecordReaders that don't return same key & value "
                + "objects. current reader class : " + realReader.getClass());

            // other alternative is to copy key and value objects, unfortunately
            // we need to pay that cost even for reader that reuse the object.
            // good compromise is to let this be set by config. we can add that
            // when required.
          }

          return true;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      eof = true; // strictly not required, just for consistency
      return false;
    }
  }

}
