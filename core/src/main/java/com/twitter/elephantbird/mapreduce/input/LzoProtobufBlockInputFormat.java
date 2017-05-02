package com.twitter.elephantbird.mapreduce.input;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * This is the base class for all blocked protocol buffer based input formats.  That is, if you use
 * the ProtobufBlockWriter to write your data, this input format can read it.
 * <br> <br>
 *
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 *
 * @Deprecated use {@link MultiInputFormat}
 */
public class LzoProtobufBlockInputFormat<M extends Message> extends MultiInputFormat<M> {

  public LzoProtobufBlockInputFormat() {
  }

  public LzoProtobufBlockInputFormat(TypeRef<M> typeRef) {
    super(typeRef);
  }

  public static<M extends Message> LzoProtobufBlockInputFormat<M> newInstance(TypeRef<M> typeRef) {
    return new LzoProtobufBlockInputFormat<M>(typeRef);
  }
}
