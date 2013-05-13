package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.input.RCFileProtobufTupleInputFormat;
import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.RCFileUtil;

/**
 * Pig loader for Protobufs stored in RCFiles.
 */
public class RCFileProtobufPigLoader extends ProtobufPigLoader<Message> {

  private RCFileProtobufTupleInputFormat.TupleReader protoReader;

  /**
   * @param protoClassName fully qualified name of the protobuf class
   */
  public RCFileProtobufPigLoader(String protoClassName) {
    super(protoClassName);
  }

  @Override @SuppressWarnings("unchecked")
  public InputFormat getInputFormat() throws IOException {
    return new RCFileProtobufTupleInputFormat(typeRef);
  }

  @Override
  public Tuple getNext() throws IOException {
    if (protoReader.isReadingUnknonwsColumn()) {
      //do normal bytes -> protobuf message -> tuple
      return super.getNext();
    }

    // otherwise bytes -> tuple
    try {
      if (protoReader.nextKeyValue()) {
        return protoReader.getCurrentTupleValue();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return null;
  }

  @Override @SuppressWarnings("unchecked")
  public void prepareToRead(RecordReader reader, PigSplit split) {
    super.prepareToRead(reader, split);
    protoReader = (RCFileProtobufTupleInputFormat.TupleReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    super.setLocation(location, job);
    RCFileUtil.setRequiredFieldConf(HadoopCompat.getConfiguration(job),
                                    requiredFieldList);
  }

}
