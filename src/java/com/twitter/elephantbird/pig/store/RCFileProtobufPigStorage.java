package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.mapreduce.output.RCFileOutputFormat;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * TODO
 * @author raghu
 */
public class RCFileProtobufPigStorage extends StoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(RCFileProtobufPigStorage.class);

  private TypeRef<? extends Message> typeRef;
  private Builder msgBuilder;
  private Builder[] fieldBuilders; // one builder for each of the non-primitive fields.

  RecordWriter<NullWritable, Writable> writer = null;

  public RCFileProtobufPigStorage(String protoClassName) {
    typeRef = Protobufs.getTypeRef(protoClassName);
    msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());

    List<Builder> columnBuilders = Lists.newArrayList();

    List<FieldDescriptor> fieldDescriptors = msgBuilder.getDescriptorForType().getFields();
    for (FieldDescriptor desc : msgBuilder.getDescriptorForType().getFields()) {

    }
  }

  @Override @SuppressWarnings("unchecked")
  public OutputFormat getOutputFormat() throws IOException {
    return new RCFileOutputFormat();
  }


  @Override @SuppressWarnings("unchecked")
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void putNext(Tuple t) throws IOException {
   Message msg = PigToProtobuf.tupleToMessage(msgBuilder, t);

  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    // set compression, columns etc.
  }

  public static class RCFileProtobufOutputFormat<M> extends RCFileOutputFormat {


  }

}
