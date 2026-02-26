package com.twitter.elephantbird.crunch;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.twitter.elephantbird.mapreduce.io.ThriftBlockReader;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.AutoClosingIterator;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.util.Iterator;

class ThriftFileReaderFactory<T extends TBase<?, ?>> implements FileReaderFactory<T> {

  private final PType<T> ptype;
  public ThriftFileReaderFactory(PType<T> ptype) {
    this.ptype = ptype;
  }

  @Override
  public Iterator<T> read(FileSystem fs, Path path) {
    try {
      final FSDataInputStream in = fs.open(path);
      return new AutoClosingIterator<T>(in, new UnmodifiableIterator<T>() {
        TypeRef<T> typeRef = new TypeRef<T>(ptype.getTypeClass()) {};
        ThriftBlockReader<T> reader = new ThriftBlockReader<T>(in, typeRef);
        ThriftWritable<T> tw = new ThriftWritable<T>(typeRef);

        @Override
        public boolean hasNext() {
          try {
            return reader.readNext(tw);
          } catch (IOException e) {
            //TODO
            return false;
          }
        }

        @Override
        public T next() {
          return tw.get();
        }
      });
    } catch (IOException e) {
      //TODO
      return Iterators.emptyIterator();
    }
  }
}
