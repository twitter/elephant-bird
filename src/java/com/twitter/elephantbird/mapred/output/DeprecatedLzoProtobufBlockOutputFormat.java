package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
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

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the output format correctly.
   */
  public static void setClassConf(Class<? extends Message> protoClass, Configuration conf) {
    Protobufs.setClassConf(conf, DeprecatedLzoProtobufBlockOutputFormat.class, protoClass);
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
