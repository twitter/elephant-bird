package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
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
 * Output format for lzo compressed thrift.
 *
 */
public class DeprecatedLzoThriftBlockOutputFormat<M extends TBase<?, ?>>
    extends DeprecatedLzoOutputFormat<NullWritable, ThriftWritable<M>> {
  @SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>> Class<DeprecatedLzoThriftBlockOutputFormat>
     getOutputFormatClass(Class<M> thriftClass, Configuration jobConf) {

    ThriftUtils.setClassConf(jobConf, DeprecatedLzoThriftBlockOutputFormat.class, thriftClass);
    return DeprecatedLzoThriftBlockOutputFormat.class;
  }

  @Override
  public RecordWriter<NullWritable, ThriftWritable<M>> getRecordWriter(
      FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable)
      throws IOException {

    TypeRef<M> typeRef = ThriftUtils.getTypeRef(jobConf, DeprecatedLzoThriftBlockOutputFormat.class);
    return new DeprecatedLzoThriftBlockRecordWriter<M>(
      new ThriftBlockWriter<M>(getOutputStream(jobConf), typeRef.getRawClass())
    );
  }
}
