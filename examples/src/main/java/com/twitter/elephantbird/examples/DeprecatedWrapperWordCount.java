package com.twitter.elephantbird.examples;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Implements Word Count using {@link DeprecatedInputFormatWrapper}
 * and {@link DeprecatedOutputFormatWrapper}.
 */
public class DeprecatedWrapperWordCount {

  private DeprecatedWrapperWordCount() {}

  public static class WordCountMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, LongWritable> {
    private final LongWritable one = new LongWritable(1L);
    private final Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, LongWritable> collector,
                    Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        collector.collect(word, one);
      }
    }
  }

  public static class WordCountReducer extends MapReduceBase
      implements Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable longWritable = new LongWritable(0);

    @Override
    public void reduce(Text key, Iterator<LongWritable> values,
                       OutputCollector<Text, LongWritable> collector,
                       Reporter reporter) throws IOException {

      long total = 0;
      while (values.hasNext()) {
        total += values.next().get();
      }

      longWritable.set(total);
      collector.collect(key, longWritable);
    }
  }

  public static void main(String[] args) throws Exception {

    System.out.println("CLASSPATH: " + System.getProperty("CLASSPATH"));

    GenericOptionsParser options = new GenericOptionsParser(args);
    args = options.getRemainingArgs();

    if (args.length != 2) {
      System.err.println("Usage: hadoop jar path/to/this.jar " +
          DeprecatedWrapperWordCount.class + " <input dir> <output dir>");
      System.exit(1);
    }

    JobConf job = new JobConf(options.getConfiguration());
    job.setJobName("Deprecated Wrapper Word Count");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setJarByClass(DeprecatedWrapperWordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);

    job.setInputFormat(DeprecatedInputFormatWrapper.class);
    DeprecatedInputFormatWrapper.setInputFormat(TextInputFormat.class, job);

    job.setOutputFormat(DeprecatedOutputFormatWrapper.class);
    DeprecatedOutputFormatWrapper.setOutputFormat(TextOutputFormat.class, job);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    JobClient.runJob(job).waitForCompletion();
  }
}
