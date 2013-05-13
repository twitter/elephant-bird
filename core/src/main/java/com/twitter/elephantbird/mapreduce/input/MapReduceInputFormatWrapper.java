package com.twitter.elephantbird.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.twitter.elephantbird.util.HadoopUtils;

/**
 * The wrapper enables an {@link InputFormat} written for old deprecated
 * <code>mapred</code> interface to be used unmodified in contexts where
 * a InputFormat with new <code>mapreduce</code> interface is required. </p>
 *
 * Usage: <pre>
 *    // set InputFormat class using a mapred InputFormat
 *    MapReduceInputFormatWrapper.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class, job);
 *    job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
 *    // ...
 * </pre>
 *
 * This is a mirror image of {@link DeprecatedInputFormatWrapper}
 *
 * @author Raghu Angadi
 *
 */
@SuppressWarnings("deprecation")
public class MapReduceInputFormatWrapper<K, V> extends org.apache.hadoop.mapreduce.InputFormat<K, V> {

  private static final String CLASS_CONF_KEY = "elephantbird.class.for.MapReduceInputFormatWrapper";

  protected InputFormat<K, V> realInputFormat;


  /**
   * Sets jobs input format to {@link MapReduceInputFormatWrapper} and stores
   * supplied real {@link InputFormat} class name in job configuration.
   * This configuration is read on the remote tasks to instantiate actual
   * InputFormat correctly.
   */
  public static void setInputFormat(Class<?> realInputFormatClass, Job job) {
    job.setInputFormatClass(MapReduceInputFormatWrapper.class);
    HadoopUtils.setClassConf(HadoopCompat.getConfiguration(job), CLASS_CONF_KEY, realInputFormatClass);
  }

  @SuppressWarnings("unchecked")
  private void initInputFormat(Configuration conf) {
    if (realInputFormat == null) {
      realInputFormat = ReflectionUtils.newInstance(
                          conf.getClass(CLASS_CONF_KEY, null, InputFormat.class),
                          conf);
    }
  }

  public MapReduceInputFormatWrapper() {
    // real inputFormat is initialized based on conf.
  }

  public MapReduceInputFormatWrapper(InputFormat<K, V> realInputFormat) {
    this.realInputFormat = realInputFormat;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context)
                                               throws IOException, InterruptedException {

    initInputFormat(HadoopCompat.getConfiguration(context));
    return new RecordReaderWrapper<K, V>(realInputFormat);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context)
                                    throws IOException, InterruptedException {

    JobConf jobConf = (JobConf) HadoopCompat.getConfiguration(context);

    initInputFormat(jobConf);

    org.apache.hadoop.mapred.InputSplit[] splits =
      realInputFormat.getSplits(jobConf, jobConf.getNumMapTasks());

    if (splits == null) {
      return null;
    }

    List<InputSplit> resultSplits = new ArrayList<InputSplit>(splits.length);

    for (org.apache.hadoop.mapred.InputSplit split : splits) {
      if (split.getClass() == org.apache.hadoop.mapred.FileSplit.class) {
        org.apache.hadoop.mapred.FileSplit mapredFileSplit =
          ((org.apache.hadoop.mapred.FileSplit)split);
        resultSplits.add(new FileSplit(mapredFileSplit.getPath(),
                                       mapredFileSplit.getStart(),
                                       mapredFileSplit.getLength(),
                                       mapredFileSplit.getLocations()));
      } else {
        resultSplits.add(new InputSplitWrapper(split));
      }
    }

    return resultSplits;
  }

  private static class RecordReaderWrapper<K, V> extends RecordReader<K, V> {


    private org.apache.hadoop.mapred.RecordReader<K, V> realReader;
    private InputFormat<K, V> realInputFormat;

    private K keyObj = null;
    private V valueObj = null;

    public RecordReaderWrapper(InputFormat<K, V> realInptuFormat) throws IOException {
      this.realInputFormat = realInptuFormat;
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return realReader.getProgress();
    }


    @Override
    public void initialize(InputSplit split, final TaskAttemptContext context)
        throws IOException, InterruptedException {

      org.apache.hadoop.mapred.InputSplit oldSplit;

      if (split.getClass() == FileSplit.class) {
        oldSplit = new org.apache.hadoop.mapred.FileSplit(
                        ((FileSplit)split).getPath(),
                        ((FileSplit)split).getStart(),
                        ((FileSplit)split).getLength(),
                        split.getLocations());
      } else {
        oldSplit = ((InputSplitWrapper)split).realSplit;
      }

      @SuppressWarnings("unchecked")
      Reporter reporter = new Reporter() { // Reporter interface over ctx

        final TaskInputOutputContext ioCtx =
                context instanceof TaskInputOutputContext ?
                       (TaskInputOutputContext) context : null;

        public void progress() { context.progress(); }

        // @Override
        public float getProgress() {
          return (ioCtx != null) ? ioCtx.getProgress() : 0;
        }

        public void setStatus(String status) {
          if (ioCtx != null)
            HadoopCompat.setStatus(ioCtx, status);
        }

        public void incrCounter(String group, String counter, long amount) {
          if (ioCtx != null)
            HadoopCompat.incrementCounter(ioCtx.getCounter(group, counter), amount);
        }

        @SuppressWarnings("unchecked")
        public void incrCounter(Enum<?> key, long amount) {
          if (ioCtx != null)
            HadoopCompat.incrementCounter(ioCtx.getCounter(key), amount);
        }

        public org.apache.hadoop.mapred.InputSplit getInputSplit()
            throws UnsupportedOperationException {
          throw new UnsupportedOperationException();
        }

        public Counter getCounter(String group, String name) {
          return ioCtx != null ?
            (Counter) HadoopCompat.getCounter(ioCtx, group, name) : null;
        }

        @SuppressWarnings("unchecked")
        public Counter getCounter(Enum<?> name) {
          return ioCtx != null ?
             (Counter)ioCtx.getCounter(name) : null;
        }
      };

      realReader = realInputFormat.getRecordReader(
                      oldSplit,
                      (JobConf) HadoopCompat.getConfiguration(context),
                      reporter);

      keyObj = realReader.createKey();
      valueObj = realReader.createValue();
    }


    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return keyObj;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return valueObj;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return realReader.next(keyObj, valueObj);
    }

  }

  private static class InputSplitWrapper extends InputSplit implements Writable {

    org.apache.hadoop.mapred.InputSplit realSplit;


    @SuppressWarnings("unused") // MapReduce instantiates this.
    public InputSplitWrapper() {}

    public InputSplitWrapper(org.apache.hadoop.mapred.InputSplit realSplit) {
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() throws IOException {
      return realSplit.getLength();
    }

    @Override
    public String[] getLocations() throws IOException {
      return realSplit.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String className = WritableUtils.readString(in);
      Class<?> splitClass;

      try {
        splitClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      realSplit = (org.apache.hadoop.mapred.InputSplit)
                  ReflectionUtils.newInstance(splitClass, null);
      ((Writable)realSplit).readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, realSplit.getClass().getName());
      ((Writable)realSplit).write(out);
    }
  }
}
