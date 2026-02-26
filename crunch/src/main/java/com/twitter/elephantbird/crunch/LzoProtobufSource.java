package com.twitter.elephantbird.crunch;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat;
import org.apache.crunch.ReadableData;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * A Crunch {@code Source} for reading protocol buffers from an {@link LzoProtobufBlockInputFormat} file. The
 * format implements the {@code ReadableSource} interface, so records from these files can be read for
 * in-memory joins.
 */
public class LzoProtobufSource<T extends Message> extends FileSourceImpl<T> implements ReadableSource<T> {

  /**
   * Factory method for creating a new {@code LzoProtobufSource} from a given path and protocol buffer
   * message class.
   *
   * @param path path to the data
   * @param protoClass the Message class to read
   * @return a new {@code LzoProtobufSource}
   */
  public static <S extends Message> LzoProtobufSource<S> at(Path path, Class<S> protoClass) {
    return new LzoProtobufSource<S>(path, EBTypes.protos(protoClass));
  }

  /**
   * Factory method for creating a new {@code LzoProtobufSource} from the given paths and protocol buffer
   * message class.
   *
   * @param paths paths to the data
   * @param protoClass the Message class to read
   * @return a new {@code LzoProtobufSource}
   */
  public static <S extends Message> LzoProtobufSource<S> at(List<Path> paths, Class<S> protoClass) {
    return new LzoProtobufSource<S>(paths, EBTypes.protos(protoClass));
  }

  private static <T> FormatBundle<LzoProtobufBlockInputFormat> getBundle(PType<T> ptype) {
    return FormatBundle.forInput(LzoProtobufBlockInputFormat.class)
        .set("elephantbird.class.for.MultiInputFormat", ptype.getTypeClass().getName());
  }

  public LzoProtobufSource(Path path, PType<T> ptype) {
    super(path, ptype, getBundle(ptype));
  }

  public LzoProtobufSource(List<Path> paths, PType<T> ptype) {
    super(paths, ptype, getBundle(ptype));
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, new ProtobufFileReaderFactory<T>(ptype));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new ProtobufReadableData<T>(paths, ptype);
  }
}
