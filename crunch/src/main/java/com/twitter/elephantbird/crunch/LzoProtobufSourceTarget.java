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
  public LzoProtobufSourceTarget(Path path, PType<T> ptype) {
    super(new LzoProtobufSource<T>(path, ptype), new LzoProtobufTarget(path));
  }
}
