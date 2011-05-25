package com.twitter.elephantbird.mapred.input;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it. It has two template
 * parameters, the protobuf and the writable for that protobuf.  This class cannot be instantiated
 * directly as an input format because Hadoop works via reflection, and Java type erasure makes it
 * impossible to instantiate a templatized class via reflection with the correct template parameter.
 * Instead, we codegen derived input format classes for any given protobuf which instantiate the
 * template parameter directly, as well as set the typeRef argument so that the template
 * parameter can be remembered.  See com.twitter.elephantbird.proto.HadoopProtoCodeGenerator.
 *
 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style
 * which is deprecated but still required in places.  Streaming, for example,
 * does a check that the given input format is a descendant of
 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
 * from the new API fails.
 */

@SuppressWarnings("deprecation")
public class DeprecatedLzoProtobufBlockInputFormat<M extends Message, W extends ProtobufWritable<M>> extends DeprecatedLzoInputFormat<M, W> {
  private TypeRef typeRef_;
  private W protobufWritable_;

  public void setTypeRef(TypeRef typeRef) {
    typeRef_ = typeRef;
  }

  protected void setProtobufWritable(W protobufWritable) {
    protobufWritable_ = protobufWritable;
  }

  /**
   * Returns {@link DeprecatedLzoProtobufBlockInputFormat} class.
   * Sets an internal configuration in jobConf so that remote Tasks
   * instantiate appropriate object based on protoClass.
   */
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<DeprecatedLzoProtobufBlockInputFormat>
     getInputFormatClass(Class<M> protoClass, JobConf jobConf) {
    Protobufs.setClassConf(jobConf, DeprecatedLzoProtobufBlockInputFormat.class, protoClass);
    return DeprecatedLzoProtobufBlockInputFormat.class;
  }

  @Override
  public RecordReader<M, W> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
    if (typeRef_ == null) {
      typeRef_ = Protobufs.getTypeRef(jobConf, DeprecatedLzoProtobufBlockInputFormat.class);
    }
    if(protobufWritable_ == null) {
      protobufWritable_ = (W) new ProtobufWritable<M>(typeRef_);
    }
    return new DeprecatedLzoProtobufBlockRecordReader(typeRef_, protobufWritable_, jobConf, (FileSplit) inputSplit);
  }
}
