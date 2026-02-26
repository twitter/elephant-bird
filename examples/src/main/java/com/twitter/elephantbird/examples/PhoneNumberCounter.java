package com.twitter.elephantbird.examples;

import java.io.IOException;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person.PhoneNumber;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For a file of LZO-compressed protobufs of type Person (see address_book.proto), generate a count of
 * phone numbers for each person.
 *
 * To write the data file, use a ProtobufBlockWriter<Person> as specified in the documentation for
 * ProtobufBlockWriter.java.  It will write blocks of 100 Person protobufs at a time to a file which will
 * be lzo compressed.
 */
public class PhoneNumberCounter extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(PhoneNumberCounter.class);

  private PhoneNumberCounter() {}

  public static class PhoneNumberCounterMapper extends Mapper<LongWritable, ProtobufWritable<Person>, Text, Text> {
    private final Text name_ = new Text();
    private final Text phoneNumber_ = new Text();

    @Override
    protected void map(LongWritable key, ProtobufWritable<Person> value, Context context) throws IOException, InterruptedException {
      Person p = value.get();

      name_.set(p.getName());
      for (PhoneNumber phoneNumber: p.getPhoneList()) {
        // Could do something with the PhoneType here; note that the protobuf data structure is fully preserved
        // inside the writable, including nesting.
        // PhoneType type = phoneNumber.getType()
        phoneNumber_.set(phoneNumber.getNumber());
        context.write(name_, phoneNumber_);
      }
    }
  }

  public static class PhoneNumberCounterReducer extends Reducer<Text, Text, Text, LongWritable> {
    private final LongWritable count_ = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (Text phoneNumber: values) {
        count++;
      }
      count_.set(count);
      context.write(key, count_);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: hadoop jar path/to/this.jar " + getClass() + " <input dir> <output dir>");
      System.exit(1);
    }

    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJobName("Protobuf Person Phone Number Counter");

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);

    job.setJarByClass(getClass());
    job.setMapperClass(PhoneNumberCounterMapper.class);
    job.setCombinerClass(PhoneNumberCounterReducer.class);
    job.setReducerClass(PhoneNumberCounterReducer.class);

    // tell inputformat the name of protbuf class corresponding to input records.
    MultiInputFormat.setClassConf(Person.class, conf);

    job.setInputFormatClass(MultiInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PhoneNumberCounter(), args);
    System.exit(exitCode);
  }
}
