package com.twitter.elephantbird.crunch;

import com.google.protobuf.Message;
import org.apache.crunch.io.impl.ReadableSourceTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;

/**
 * A Crunch {@code SourceTarget} for writing files out using a
 * {@link com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat} and then reading them back with a
 * {@link com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat}.
 */
public class LzoProtobufSourceTarget<T extends Message> extends ReadableSourceTargetImpl<T> {

  /**
   * Factory method for creating a new {@code LzoProtobufSourceTarget} from a given path and protocol buffer
   * message class.
   *
   * @param path path to the data
   * @param protoClass the Message class to read
   * @return a new {@code LzoProtobufSourceTarget}
   */
  public static <S extends Message> LzoProtobufSourceTarget<S> at(Path path, Class<S> protoClass) {
    return new LzoProtobufSourceTarget<S>(path, EBTypes.protos(protoClass));
  }

  public LzoProtobufSourceTarget(Path path, PType<T> ptype) {
    super(new LzoProtobufSource<T>(path, ptype), new LzoProtobufTarget(path));
  }
}
