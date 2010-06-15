package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.mapreduce.io.ThriftB64LineWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;

/**
 * This is the output format class for base64 encoded, line-oriented thrift based formats. Data is
 * written as one base64 encoded serialized thrift object per line. It takes one template parameter, the 
 * thrift type. This parameter is saved in a TypeRef for use in the getRecordWriter factory method.
 */
public class LzoThriftB64LineOutputFormat<T extends TBase>
    extends FileOutputFormat<NullWritable, ThriftB64LineWritable<T>> {
  private static final Logger LOG = LogManager.getLogger(LzoThriftB64LineOutputFormat.class);

  protected TypeRef<T> typeRef_;

  protected void setTypeRef(TypeRef<T> typeRef) {
    typeRef_ = typeRef;
  }

  public RecordWriter getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    LzopCodec codec = new LzopCodec();
    codec.setConf(conf);

    Path file = getDefaultWorkFile(job, codec.getDefaultExtension());
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);

    return new LzoThriftB64LineRecordWriter<T>(typeRef_,
            new DataOutputStream(codec.createOutputStream(fileOut)));
  }
}
