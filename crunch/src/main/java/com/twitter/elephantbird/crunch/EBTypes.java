package com.twitter.elephantbird.crunch;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.thrift.TBase;

/**
 * Crunch {@link org.apache.crunch.types.PType} implementations for Elephant Bird's {@code Writable} types.
 */
public class EBTypes {

  public static <M extends Message> PType<M> protos(Class<M> protoClass) {
    return Writables.derived(protoClass, new BinaryInFn(), new ProtoOutFn(protoClass),
        Writables.writables(ProtobufWritable.class));
  }

  public static <M extends TBase<?, ?>> PType<M> thrifts(Class<M> thriftClass) {
    return Writables.derived(thriftClass, new BinaryInFn(), new ThriftOutFn(thriftClass),
        Writables.writables(ProtobufWritable.class));
  }

  private static class BinaryInFn<T, W extends BinaryWritable<T>> extends MapFn<W, T> {
    @Override
    public T map(W input) {
      return input.get();
    }
  }

  private static class ProtoOutFn<T extends Message> extends MapFn<T, ProtobufWritable<T>> {
    private final Class<T> clazz;

    public ProtoOutFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public ProtobufWritable<T> map(T input) {
      ProtobufWritable<T> w = ProtobufWritable.newInstance(clazz);
      w.set(input);
      return w;
    }
  }

  private static class ThriftOutFn<T extends TBase<?, ?>> extends MapFn<T, ThriftWritable<T>> {
    private final Class<T> clazz;

    public ThriftOutFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public ThriftWritable<T> map(T input) {
      ThriftWritable<T> w = ThriftWritable.newInstance(clazz);
      w.set(input);
      return w;
    }
  }
}
