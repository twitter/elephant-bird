package com.twitter.elephantbird.examples.proto.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.examples.proto.mapreduce.input.LzoPersonProtobufB64LineInputFormat;
import com.twitter.elephantbird.mapreduce.input.LzoLineRecordReader;
import com.twitter.elephantbird.pig.load.LzoProtobufB64LinePigLoader;
import com.twitter.elephantbird.util.TypeRef;

public class LzoPersonProtobufB64LinePigLoader extends LzoProtobufB64LinePigLoader<Person> {
  public LzoPersonProtobufB64LinePigLoader() {
    setTypeRef(new TypeRef<Person>(){});
  }
  public void setLocation(String location, Job job)
  throws IOException {
	  FileInputFormat.setInputPaths(job, location);
  }
  public InputFormat getInputFormat() {
      return new LzoPersonProtobufB64LineInputFormat();
  }

  public void prepareToRead(RecordReader reader, PigSplit split) {
	  is_ = (LzoLineRecordReader)reader;
      
      
  }
}

