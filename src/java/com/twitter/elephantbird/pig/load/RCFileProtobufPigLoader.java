package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

/**
 * Pig loader for Protobufs stored in RCFiles.
 */
public class RCFileProtobufPigLoader extends LzoProtobufB64LinePigLoader<Message> {

  /**
   * @param protoClassName fully qualified name of the protobuf class
   */
  public RCFileProtobufPigLoader(String protoClassName) {
    super(protoClassName);
  }

  @Override
  public InputFormat<LongWritable, ProtobufWritable<Message>> getInputFormat() throws IOException {
    // create the InputFormat.
    return null;
  }

}
