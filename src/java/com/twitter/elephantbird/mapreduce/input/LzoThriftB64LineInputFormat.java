package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.twitter.elephantbird.mapreduce.input.LzoInputFormat;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.mapreduce.io.ThriftB64LineWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;

/**
 * This is the base class for all base64 encoded, line-oriented thrift based input formats.
 * Data is expected to be one base64 encoded serialized thrift message per line. It has two template
 * parameters, the thrift type and the thrift writable for that type.  This class cannot be instantiated
 * directly as an input format because Hadoop works via reflection, and Java type erasure makes it
 * impossible to instantiate a templatized class via reflection with the correct template parameter.
 * Instead, we codegen derived input format classes for any given thrift object which instantiate the
 * template parameter directly, as well as set the typeRef argument so that the template
 * parameter can be remembered. 
 */

public class LzoThriftB64LineInputFormat <T extends TBase, W extends ThriftB64LineWritable<T>> extends LzoInputFormat<LongWritable, W> {
  private TypeRef<T> typeRef_;
  private W thriftWritable_;

  public LzoThriftB64LineInputFormat() {
  }

  protected void setTypeRef(TypeRef<T> typeRef) {
    typeRef_ = typeRef;
  }

  protected void setThriftWritable(W thriftWritable) {
    thriftWritable_ = thriftWritable;
  }

  @Override
  public RecordReader<LongWritable, W> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    return new LzoThriftB64LineRecordReader<T, W>(typeRef_, thriftWritable_);
  }
}
