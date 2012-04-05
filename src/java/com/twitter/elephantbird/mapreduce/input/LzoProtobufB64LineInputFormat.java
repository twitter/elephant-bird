package com.twitter.elephantbird.mapreduce.input;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all base64 encoded, line-oriented protocol buffer based input formats.
 * Data is expected to be one base64 encoded serialized protocol buffer per line.
 * <br><br>
 *
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 *
 * @Deprecated use {@link MultiInputFormat}
 */
public class LzoProtobufB64LineInputFormat<M extends Message> extends MultiInputFormat<M> {

  public LzoProtobufB64LineInputFormat() {
  }

  public LzoProtobufB64LineInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }

  public static<M extends Message> LzoProtobufB64LineInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufB64LineInputFormat<M>(typeRef);
  }
}
