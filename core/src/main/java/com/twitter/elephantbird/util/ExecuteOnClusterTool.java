package com.twitter.elephantbird.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for running a method as a single mapper on the hadoop cluster
 * Useful for running a job that isn't really a map reduce job on the cluster
 *
 * @author Alex Levenson
 */
public abstract class ExecuteOnClusterTool extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(ExecuteOnClusterTool.class);
  private static final String IMPL_KEY = ExecuteOnClusterTool.class.getName() + ".implclass";

  /**
   * Override if you need to store any of the commandline args into the job conf
   *
   * @param args passed to this tool
   * @param conf job conf
   */
  protected void setup(String[] args, Configuration conf) throws IOException { }

  /**
   * This will be called once from a single mapper
   * A heartbeat thread will be started (and stopped) for you, so this method
   * can be slow, the task will not fail due to timing out.
   *
   * @param context the mapper's context
   * @throws IOException
   */
  public abstract void execute(Mapper.Context context) throws IOException;

  @Override
  public int run(String[] args) throws Exception {
    setup(args, getConf());

    getConf().set(IMPL_KEY, getClass().getName());
    Job job = new Job(getConf());
    job.setInputFormatClass(DummyInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapperClass(ExecuteOnClusterMapper.class);
    job.setNumReduceTasks(0);
    job.setJarByClass(getClass());
    job.submit();
    return job.waitForCompletion(true) ? 0 : -1;
  }

  private static final class ExecuteOnClusterMapper
      extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    protected void map(NullWritable key, NullWritable value, Context context)
      throws IOException, InterruptedException {
      ExecuteOnClusterTool tool;
      try {
        tool = (ExecuteOnClusterTool) Class.forName(
            HadoopCompat.getConfiguration(context).get(IMPL_KEY)).newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      TaskHeartbeatThread beat = new TaskHeartbeatThread(context) {
        @Override
        protected void progress() {
          LOG.info("Sending heartbeat");
        }
      };
      try {
        beat.start();
        tool.execute(context);
      } finally {
        beat.stop();
      }
    }
  }

  private static final class DummyInputSplit extends InputSplit implements Writable {

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }
  }

  private static final class DummyInputFormat extends InputFormat<NullWritable, NullWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
      return Lists.<InputSplit>newArrayList(new DummyInputSplit());
    }

    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      return new DummyRecordReader();
    }
  }

  private static final class DummyRecordReader extends RecordReader<NullWritable, NullWritable> {
    private boolean first = true;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (first) {
        first = false;
        return true;
      }
      return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
