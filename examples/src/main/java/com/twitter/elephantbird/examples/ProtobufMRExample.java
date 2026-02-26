package com.twitter.elephantbird.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
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

import com.twitter.elephantbird.examples.proto.Examples.Age;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;

/**
 * -Dproto.test=lzoOut : takes text files with name and age on each line as
 * input and writes to lzo file with Protobuf serilized data. <br>
 * -Dproto.test=lzoIn : does the reverse. <br><br>
 *
 * -Dproto.test.format=Block (or B64Line) to test different formats. <br>
 */

public class ProtobufMRExample {
  // This is intentionally very similar to ThriftMRExample.

  private ProtobufMRExample() {}

  public static class TextMapper extends Mapper<LongWritable, Text, NullWritable, ProtobufWritable<Age>> {
    ProtobufWritable<Age> protoWritable = ProtobufWritable.newInstance(Age.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer line = new StringTokenizer(value.toString(), "\t\r\n");
      String name;

      if (line.hasMoreTokens()
          && (name = line.nextToken()) != null
          && line.hasMoreTokens()) {
          protoWritable.set(Age.newBuilder()
                              .setName(name)
                              .setAge(Integer.parseInt(line.nextToken()))
                              .build());
          context.write(null, protoWritable);
      }
    }
  }

  public int runTextToLzo(String[] args, Configuration conf) throws Exception {
    Job job = new Job(conf);
    job.setJobName("Protobuf Example : Text to LzoB64Line");

    job.setJarByClass(getClass());
    job.setMapperClass(TextMapper.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    if (conf.get("proto.test.format", "B64Line").equals("Block")) {
      LzoProtobufBlockOutputFormat.setClassConf(Age.class, HadoopCompat.getConfiguration(job));
      job.setOutputFormatClass(LzoProtobufBlockOutputFormat.class);
    } else { // assume B64Line
      LzoProtobufB64LineOutputFormat.setClassConf(Age.class, HadoopCompat.getConfiguration(job));
      job.setOutputFormatClass(LzoProtobufB64LineOutputFormat.class);
    }

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class LzoMapper extends Mapper<LongWritable, ProtobufWritable<Age>, Text, Text> {
    @Override
    protected void map(LongWritable key, ProtobufWritable<Age> value, Context context) throws IOException, InterruptedException {
      Age age = value.get();
      context.write(null, new Text(age.getName() + "\t" + age.getAge()));
    }
  }

  int runLzoToText(String[] args, Configuration conf) throws Exception {
    Job job = new Job(conf);
    job.setJobName("Protobuf Example : LzoB64Line to Text");

    job.setJarByClass(getClass());
    job.setMapperClass(LzoMapper.class);
    job.setNumReduceTasks(0);

    // input format is same for both B64Line or block:
    MultiInputFormat.setInputFormatClass(Age.class, job);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class SortMapper extends Mapper<LongWritable, Text, Text, ProtobufWritable<Age>> {
    ProtobufWritable<Age> protoWritable = ProtobufWritable.newInstance(Age.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer line = new StringTokenizer(value.toString(), "\t\r\n");
      String name;

      if (line.hasMoreTokens()
          && (name = line.nextToken()) != null
          && line.hasMoreTokens()) {
          protoWritable.set(Age.newBuilder()
                              .setName(name)
                              .setAge(Integer.parseInt(line.nextToken()))
                              .build());
          context.write(new Text(name), protoWritable);
      }
    }
  }

  public static class SortReducer extends Reducer<Text, ProtobufWritable<Age>, Text, Text> {
    ProtobufConverter<Age> converter = ProtobufConverter.newInstance(Age.class);
    @Override
    protected void reduce(Text key, Iterable<ProtobufWritable<Age>> values, Context context) throws IOException, InterruptedException {
      for(ProtobufWritable<Age> value : values) {
        /* setConverter() before value.get() is invoked. It is required since
         * 'value' object was created by MR with ProtobufWritable's default
         * constructor, as result object does not know its runtime Protobuf class.
         * The 'value' object just contains serialized bytes for the protobuf.
         * It can create the actual protobuf only after it knows which protobuf
         * class to use.
         */
        value.setConverter(converter);
        context.write(null, new Text(value.get().getName() + "\t" + value.get().getAge()));
      }
    }
  }

  int runSorter(String[] args, Configuration conf) throws Exception {
    Job job = new Job(conf);
    job.setJobName("Protobuf Example : ProtobufWritable as Map output class");

    job.setJarByClass(getClass());
    job.setMapperClass(SortMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ProtobufWritable.class);

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
    ProtobufMRExample runner = new ProtobufMRExample();

    if (args.length != 2) {
      System.out.println("Usage: hadoop jar path/to/this.jar " + runner.getClass() + " <input dir> <output dir>");
      System.exit(1);
    }

    String test = conf.get("proto.test", "lzoIn");

    if (test.equals("lzoIn"))
      System.exit(runner.runLzoToText(args, conf));
    if (test.equals("lzoOut"))
      System.exit(runner.runTextToLzo(args, conf));
    if (test.equals("sort"))
      System.exit(runner.runSorter(args, conf));
  }
}
