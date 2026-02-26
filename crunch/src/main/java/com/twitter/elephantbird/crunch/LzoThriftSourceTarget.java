package com.twitter.elephantbird.crunch;

import org.apache.crunch.io.impl.ReadableSourceTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

/**
 * A Crunch {@code SourceTarget} for writing files with the
 * {@link com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat} and reading them back with the
 * {@link com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat}.
 */
public class LzoThriftSourceTarget<T extends TBase<?, ?>> extends ReadableSourceTargetImpl<T> {

  /**
   * Factory method for creating a new {@code LzoThriftSourceTarget} from a given path and Thrift
   * record class.
   *
   * @param path path to the data
   * @param thriftClass the Thrift class to read
   * @return a new {@code LzoThriftSourceTarget}
   */
  public static <S extends TBase<?, ?>> LzoThriftSourceTarget<S> at(Path path, Class<S> thriftClass) {
    return new LzoThriftSourceTarget<S>(path, EBTypes.thrifts(thriftClass));
  }

  public LzoThriftSourceTarget(Path path, PType<T> ptype) {
    super(new LzoThriftSource<T>(path, ptype), new LzoThriftTarget(path));
  }
}
