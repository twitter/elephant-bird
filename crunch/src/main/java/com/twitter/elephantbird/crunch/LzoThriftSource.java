package com.twitter.elephantbird.crunch;

import com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat;
import org.apache.crunch.ReadableData;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.util.List;

/**
 * A Crunch {@code Source} for reading Thrift records from an {@link LzoThriftBlockInputFormat} file. The
 * format implements the {@code ReadableSource} interface, so records from these files can be read for
 * in-memory joins.
 */
public class LzoThriftSource<T extends TBase<?, ?>> extends FileSourceImpl<T> implements ReadableSource<T> {

  /**
   * Factory method for creating a new {@code LzoThriftSource} from a given path and Thrift
   * record class.
   *
   * @param path path to the data
   * @param thriftClass the Thrift class to read
   * @return a new {@code LzoThriftSource}
   */
  public static <S extends TBase<?, ?>> LzoThriftSource<S> at(Path path, Class<S> thriftClass) {
    return new LzoThriftSource<S>(path, EBTypes.thrifts(thriftClass));
  }

  /**
   * Factory method for creating a new {@code LzoThriftSource} from the given paths and Thrift
   * record class.
   *
   * @param paths list of paths to data
   * @param thriftClass the Thrift class to read
   * @return a new {@code LzoThriftSource}
   */
  public static <S extends TBase<?, ?>> LzoThriftSource<S> at(List<Path> paths, Class<S> thriftClass) {
    return new LzoThriftSource<S>(paths, EBTypes.thrifts(thriftClass));
  }

  private static <T> FormatBundle<LzoThriftBlockInputFormat> getBundle(PType<T> ptype) {
    return FormatBundle.forInput(LzoThriftBlockInputFormat.class)
        .set("elephantbird.class.for.MultiInputFormat", ptype.getTypeClass().getName());
  }

  public LzoThriftSource(Path path, PType<T> ptype) {
    super(path, ptype, getBundle(ptype));
  }

  public LzoThriftSource(List<Path> paths, PType<T> ptype) {
    super(paths, ptype, getBundle(ptype));
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, new ThriftFileReaderFactory<T>(ptype));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new ThriftReadableData<T>(paths, ptype);
  }
}
