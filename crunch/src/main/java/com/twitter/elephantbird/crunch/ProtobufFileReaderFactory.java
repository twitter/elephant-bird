package com.twitter.elephantbird.crunch;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.AutoClosingIterator;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;

class ProtobufFileReaderFactory<T extends Message> implements FileReaderFactory<T> {

  private final PType<T> ptype;

  public ProtobufFileReaderFactory(PType<T> ptype) {
    this.ptype = ptype;
  }

  @Override
  public Iterator<T> read(FileSystem fs, Path path) {
    try {
      final FSDataInputStream in = fs.open(path);
      return new AutoClosingIterator<T>(in, new UnmodifiableIterator<T>() {
        TypeRef<T> typeRef = new TypeRef<T>(ptype.getTypeClass()) {};
        ProtobufBlockReader<T> reader = new ProtobufBlockReader<T>(in, typeRef);
        ProtobufWritable<T> pw = new ProtobufWritable<T>(typeRef);

        @Override
        public boolean hasNext() {
          try {
            return reader.readNext(pw);
          } catch (IOException e) {
            //TODO
            return false;
          }
        }

        @Override
        public T next() {
          return pw.get();
        }
      });
    } catch (IOException e) {
      //TODO
      return Iterators.emptyIterator();
    }
  }
}
