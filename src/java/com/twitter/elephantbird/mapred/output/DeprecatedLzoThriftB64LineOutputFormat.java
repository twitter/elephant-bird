package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TBase;

import java.io.IOException;

/**
 * Base64 encode each thrift serialization string then lzo-compress and output as a line.
 *
 * @author Yifan Shi
 */
public class DeprecatedLzoThriftB64LineOutputFormat<M extends TBase<?, ?>>
    extends DeprecatedLzoOutputFormat<NullWritable, ThriftWritable<M>> {

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the output format correctly.
   */
  public static void setClassConf(Class<? extends TBase<?, ?>> thriftClass, Configuration conf) {
    ThriftUtils.setClassConf(conf, DeprecatedLzoThriftB64LineOutputFormat.class, thriftClass);
  }

  @Override
  public RecordWriter<NullWritable, ThriftWritable<M>> getRecordWriter(
      FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable)
      throws IOException {

    TypeRef<M> typeRef = ThriftUtils.getTypeRef(jobConf, DeprecatedLzoThriftB64LineOutputFormat.class);
    return new DeprecatedLzoThriftB64LineRecordWriter<M>(
        new ThriftConverter<M>(typeRef), getOutputStream(jobConf));
  }
}
