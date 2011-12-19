package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.RCFileProtobufInputFormat;
import com.twitter.elephantbird.pig.util.RCFileUtil;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Pig loader for Protobufs stored in RCFiles.
 */
public class RCFileProtobufPigLoader extends LzoProtobufB64LinePigLoader<Message> {

  private RCFileProtobufInputFormat.ProtobufReader protoReader;

  /**
   * @param protoClassName fully qualified name of the protobuf class
   */
  public RCFileProtobufPigLoader(String protoClassName) {
    super(protoClassName);
  }

  @Override @SuppressWarnings("unchecked")
  public InputFormat getInputFormat() throws IOException {
    return new RCFileProtobufInputFormat(typeRef);
  }

  @SuppressWarnings("unchecked")
  protected <M> M getNextBinaryValue(TypeRef<M> typeRef) throws IOException {
    try {
      if (protoReader.nextKeyValue()) {
        return (M) protoReader.getCurrentProtobufValue();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return null;
  }

  @Override @SuppressWarnings("unchecked")
  public void prepareToRead(RecordReader reader, PigSplit split) {
    // pass null so that, there is no way it could be misused
    super.prepareToRead(null, split);
    protoReader = (RCFileProtobufInputFormat.ProtobufReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    super.setLocation(location, job);
    RCFileUtil.setRequiredFieldConf(job.getConfiguration(),
                                    requiredFieldList);
  }

}
