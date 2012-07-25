package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Similar to the LzoProtobufBlockInputFormat class, but instead of being specific to
 * a given protocol buffer (via codegen of a LzoProtobufBlockInputFormat-derived class),
 * this InputFormat returns <position, protobuf bytes> pairs and leaves the parsing of
 * each individual protobuf into object form to the user.
 *
 * This has two advantages.  One, if your data is composed of multiple types of protobufs which
 * you know via some other method, you need to use this to decide the type at runtime.  More
 * common are situations where you just want an aggregate over all protobufs (such as a count)
 * without caring about the individual protobuf fields, in which case this class is faster
 * because you don't pay for protobuf object deserialization.
 */

public class LzoGenericProtobufBlockInputFormat extends LzoInputFormat<LongWritable, BytesWritable> {

  public LzoGenericProtobufBlockInputFormat() {
  }

  @Override
  public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {

    return new LzoGenericProtobufBlockRecordReader();
  }
}
