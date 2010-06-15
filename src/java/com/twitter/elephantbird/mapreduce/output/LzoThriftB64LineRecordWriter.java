package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.mapreduce.io.ThriftB64LineWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;

/**
 * A RecordWriter-derived class for use with the LzoThriftB64LineOutputFormat.
 * Writes data as base64 encoded serialized thrift objects, one per line.
 */

public class LzoThriftB64LineRecordWriter<T extends TBase>
    extends RecordWriter<NullWritable, ThriftB64LineWritable<T>> {
  private static final Logger LOG = LogManager.getLogger(LzoThriftB64LineRecordWriter.class);

  protected final TypeRef typeRef_;
  protected final DataOutputStream out_;

  public LzoThriftB64LineRecordWriter(TypeRef<T> typeRef, DataOutputStream out) {
    typeRef_ = typeRef;
    out_ = out;
  }

  public void write(NullWritable nullWritable, ThriftB64LineWritable<T> thriftWritable)
      throws IOException, InterruptedException {
    thriftWritable.write(out_);
  }

  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    out_.close();
  }
}
