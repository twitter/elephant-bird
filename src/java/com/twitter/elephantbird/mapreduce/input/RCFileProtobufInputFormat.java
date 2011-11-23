package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.piggybank.storage.hiverc.HiveRCInputFormat;
import org.apache.pig.piggybank.storage.hiverc.HiveRCRecordReader;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

public class RCFileProtobufInputFormat extends HiveRCInputFormat {

  public static String REQUIRED_PROTO_FIELD_INDICES_CONF = "elephantbird.";
  private TypeRef<Message> typeRef_;

  /** interanl, for MR use only. */
  public RCFileProtobufInputFormat() {
  }

  public RCFileProtobufInputFormat(TypeRef<Message> typeRef) {
    super();
    this.typeRef_ = typeRef;
  }

  /**
   * Returns {@link RCFileProtobufInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("unchecked")
  public static Class<RCFileProtobufInputFormat>
        getInputFormatClass(Class<? extends Message> protoClass, Configuration jobConf) {
    Protobufs.setClassConf(jobConf, RCFileProtobufInputFormat.class, protoClass);
    return RCFileProtobufInputFormat.class;
  }

  private class ProtobufReader extends HiveRCRecordReader {

  }

  /*
  @Override
  public RecordReader<LongWritable, ProtobufWritable<Message>> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(taskAttempt.getConfiguration(), LzoProtobufBlockInputFormat.class);
    }
    return null;
    //return new LzoProtobufBlockRecordReader<M>(typeRef_);
  }
*/
}
