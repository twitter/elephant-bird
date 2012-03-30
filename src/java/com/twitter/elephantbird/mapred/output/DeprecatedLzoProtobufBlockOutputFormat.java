package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.google.protobuf.Message;

import java.io.IOException;

/**
 * Output format for lzo compressed thrift.
 *
 */
public class DeprecatedLzoProtobufBlockOutputFormat<M extends Message>
    extends DeprecatedLzoOutputFormat<NullWritable, ProtobufWritable<M>> {
  @SuppressWarnings("unchecked")
  public static <M extends Message> Class<DeprecatedLzoProtobufBlockOutputFormat>
     getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {

    Protobufs.setClassConf(jobConf, DeprecatedLzoProtobufBlockOutputFormat.class, protoClass);
    return DeprecatedLzoProtobufBlockOutputFormat.class;
  }

  @Override
  public RecordWriter<NullWritable, ProtobufWritable<M>> getRecordWriter(
      FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable)
      throws IOException {

    TypeRef<M> typeRef = Protobufs.getTypeRef(jobConf, DeprecatedLzoProtobufBlockOutputFormat.class);
    return new DeprecatedLzoProtobufBlockRecordWriter<M>(
      new ProtobufBlockWriter<M>(getOutputStream(jobConf), typeRef.getRawClass())
    );
  }
}
