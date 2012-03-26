package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.io.BinaryBlockReader;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftBlockReader;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

import org.apache.thrift.TBase;

import java.io.IOException;
import java.io.InputStream;

/**
 * A reader for LZO-encoded thrift blocks, generally written by
 * a ThriftBlockWriter or similar.  Returns <position, thrift> pairs.
 */
@SuppressWarnings("deprecation")
public class DeprecatedLzoThriftBlockRecordReader<M extends TBase<?,?>>
    extends DeprecatedLzoBlockRecordReader<M> {

  public DeprecatedLzoThriftBlockRecordReader(TypeRef<M> typeRef, BinaryWritable<M> writable, Configuration conf, FileSplit split) throws IOException {
    super(typeRef, writable, conf, split);
  }

  protected BinaryBlockReader<M> createInputReader(InputStream input, Configuration conf) throws IOException {
    return new ThriftBlockReader<M>(input, typeRef_);
  }
}
