package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

/**
 * Reads Thrift objects written in blocks using LzoThriftBlockOutputFormat
 * <br><br>
 *
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */
public class LzoThriftBlockInputFormat<M extends TBase<?, ?>> extends MultiInputFormat<M> {

  public LzoThriftBlockInputFormat() {}

  public LzoThriftBlockInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }
}
