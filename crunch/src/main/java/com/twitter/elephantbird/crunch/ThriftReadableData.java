package com.twitter.elephantbird.crunch;

import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.ReadableDataImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import java.util.List;

class ThriftReadableData<T extends TBase<?, ?>> extends ReadableDataImpl<T> {

  private final PType<T> ptype;

  ThriftReadableData(List<Path> paths, PType<T> ptype) {
    super(paths);
    this.ptype = ptype;
  }

  @Override
  protected FileReaderFactory<T> getFileReaderFactory() {
    return new ThriftFileReaderFactory<T>(ptype);
  }
}
