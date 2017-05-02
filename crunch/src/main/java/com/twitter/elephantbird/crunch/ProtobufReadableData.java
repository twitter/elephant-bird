package com.twitter.elephantbird.crunch;

import com.google.protobuf.Message;
import org.apache.crunch.ReadableData;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.ReadableDataImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;

import java.util.List;

class ProtobufReadableData<T extends Message> extends ReadableDataImpl<T> {
  private final PType<T> ptype;

  public ProtobufReadableData(List<Path> paths, PType<T> ptype) {
    super(paths);
    this.ptype = ptype;
  }

  @Override
  protected FileReaderFactory<T> getFileReaderFactory() {
    return new ProtobufFileReaderFactory<T>(ptype);
  }
}
