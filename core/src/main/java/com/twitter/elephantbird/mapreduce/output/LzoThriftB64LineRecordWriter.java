package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * This class is not strictly necessary, you can use LzoBinaryB64LineRecordWriter directly.<br>
 * It is just there to make the Protobuf dependency clear.
 *
 * @param <T> thrift message that will be written
 * @param <W> writable that wraps this message
 */
public class LzoThriftB64LineRecordWriter<T extends TBase<?, ?>, W extends ThriftWritable<T>>
extends LzoBinaryB64LineRecordWriter<T, W>{

  public LzoThriftB64LineRecordWriter(BinaryConverter<T> converter, DataOutputStream out) {
    super(converter, out);
  }

}
