package com.twitter.elephantbird.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a word count over the input, which is assumed to be LZO-compressed.  If the input lzo files are indexed,
 * the input format will take advantage of it.  The input file/directory is taken as the first argument,
 * and the output directory is taken as the second argument.  Uses NullWritable for efficiency.
 *
 * To generate data for use with this word counter, take any old data file, run lzop over it, and
 * place the resulting compressed file in the directory you use as the first argument to this class on
 * the command line.
 */
public class LzoWordCount extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(LzoWordCount.class);

  private LzoWordCount() {}

  public static class LzoWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final LongWritable one_ = new LongWritable(1L);
    private final Text word_ = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word_.set(tokenizer.nextToken());
        context.write(word_, one_);
      }
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: hadoop jar path/to/this.jar " + getClass() + " <input dir> <output dir>");
      System.exit(1);
    }

    Job job = new Job(getConf());
    job.setJobName("LZO Word Count");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setJarByClass(getClass());
    job.setMapperClass(LzoWordCountMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    // Use the custom LzoTextInputFormat class.
    job.setInputFormatClass(LzoTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LzoWordCount(), args);
    System.exit(exitCode);
  }
}
