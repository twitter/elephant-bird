package com.twitter.elephantbird.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.twitter.elephantbird.examples.thrift.Age;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.output.LzoThriftB64LineOutputFormat;
import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat;

/**
 * -Dthrift.test=lzoOut : takes text files with name and age on each line as
 * input and writes to lzo file with Thrift serilized data. <br>
 * -Dthrift.test=lzoIn : does the reverse. <br><br>
 *
 * -Dthrift.test.format=Block (or B64Line) to test different formats. <br>
 */

public class ThriftMRExample {

  private ThriftMRExample() {}

  public static class TextMapper extends Mapper<LongWritable, Text, NullWritable, ThriftWritable<Age>> {
    ThriftWritable<Age> tWritable = ThriftWritable.newInstance(Age.class);
    Age age = new Age();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer line = new StringTokenizer(value.toString(), "\t\r\n");
      if (line.hasMoreTokens()
          && age.setName(line.nextToken()) != null
          && line.hasMoreTokens()
          && age.setAge(Integer.parseInt(line.nextToken())) != null) {
          tWritable.set(age);
          context.write(null, tWritable);
      }
    }
  }

  public int runTextToLzo(String[] args, Configuration conf) throws Exception {
    Job job = new Job(conf);
    job.setJobName("Thrift Example : Text to LzoB64Line");

    job.setJarByClass(getClass());
    job.setMapperClass(TextMapper.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    if (conf.get("thrift.test.format", "B64Line").equals("Block")) {
      LzoThriftBlockOutputFormat.setClassConf(Age.class, HadoopCompat.getConfiguration(job));
      job.setOutputFormatClass(LzoThriftBlockOutputFormat.class);
    } else { // assume B64Line
      LzoThriftB64LineOutputFormat.setClassConf(Age.class, HadoopCompat.getConfiguration(job));
      job.setOutputFormatClass(LzoThriftB64LineOutputFormat.class);
    }

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }


  public static class LzoMapper extends Mapper<LongWritable, ThriftWritable<Age>, Text, Text> {
    @Override
    protected void map(LongWritable key, ThriftWritable<Age> value, Context context) throws IOException, InterruptedException {
      Age age = value.get();
      context.write(null, new Text(age.getName() + "\t" + age.getAge()));
    }
  }

  public int runLzoToText(String[] args, Configuration conf) throws Exception {
    Job job = new Job(conf);
    job.setJobName("Thrift Example : LzoB64Line to Text");

    job.setJarByClass(getClass());
    job.setMapperClass(LzoMapper.class);
    job.setNumReduceTasks(0);

    // input format is same for both B64Line and Block formats
    MultiInputFormat.setInputFormatClass(Age.class, job);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class SortMapper extends Mapper<LongWritable, Text, Text, ThriftWritable<Age>> {
    ThriftWritable<Age> tWritable = ThriftWritable.newInstance(Age.class);
    Age age = new Age();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer line = new StringTokenizer(value.toString(), "\t\r\n");

      if (line.hasMoreTokens()
          && age.setName(line.nextToken()) != null
          && line.hasMoreTokens()
          && age.setAge(Integer.parseInt(line.nextToken())) != null) {
          tWritable.set(age);
          context.write(new Text(age.getName()), tWritable);
      }
    }
  }

  public static class SortReducer extends Reducer<Text, ThriftWritable<Age>, Text, Text> {
     ThriftConverter<Age> converter = ThriftConverter.newInstance(Age.class);

    @Override
    protected void reduce(Text key, Iterable<ThriftWritable<Age>> values, Context context) throws IOException, InterruptedException {
      for(ThriftWritable<Age> value : values) {
        /* setConverter() before get() is required since 'value' object was
         * created by MR with default ThriftWritable's default constructor,
         * as result object does not know its runtime Thrift class.
         */
        value.setConverter(converter);
        context.write(null, new Text(value.get().getName() + "\t" + value.get().getAge()));
      }
    }
  }

  int runSorter(String[] args, Configuration conf) throws Exception {
    //A more complete example with reducers. Tests ThriftWritable as
    //map output value class.
    Job job = new Job(conf);
    job.setJobName("Thift Example : ThriftWritable as Map output class");

    job.setJarByClass(getClass());
    job.setMapperClass(SortMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ThriftWritable.class);

    job.setReducerClass(SortReducer.class);
    job.setNumReduceTasks(1);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();
    ThriftMRExample runner = new ThriftMRExample();

    if (args.length != 2) {
      System.out.println("Usage: hadoop jar path/to/this.jar " + runner.getClass() + " <input dir> <output dir>");
      System.exit(1);
    }

    String test = conf.get("thrift.test", "lzoIn");

    if (test.equals("lzoIn"))
      System.exit(runner.runLzoToText(args, conf));
    if (test.equals("lzoOut"))
      System.exit(runner.runTextToLzo(args, conf));
    if (test.equals("sort"))
      System.exit(runner.runSorter(args, conf));
  }
}
