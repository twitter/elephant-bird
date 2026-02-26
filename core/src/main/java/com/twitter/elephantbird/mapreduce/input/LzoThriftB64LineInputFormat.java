package com.twitter.elephantbird.mapreduce.input;

import org.apache.thrift.TBase;

import com.twitter.elephantbird.util.TypeRef;

/**
 * Reads line from an lzo compressed text file, base64 decodes it, and then
 * deserializes that into the Thrift object.
 * Returns <position, thriftObject> pairs. <br><br>
 *
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 *
 * @Deprecated use {@link MultiInputFormat}
 */
public class LzoThriftB64LineInputFormat<M extends TBase<?, ?>> extends MultiInputFormat<M> {

  public LzoThriftB64LineInputFormat() {}

  public LzoThriftB64LineInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }
}
