package com.twitter.elephantbird.crunch;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
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
    return Writables.derived(protoClass, new ProtoInFn(protoClass), new ProtoOutFn(protoClass),
        Writables.writables(ProtobufWritable.class));
  }

  public static <M extends TBase<?, ?>> PType<M> thrifts(Class<M> thriftClass) {
    return Writables.derived(thriftClass, new ThriftInFn(thriftClass), new ThriftOutFn(thriftClass),
        Writables.writables(ThriftWritable.class));
  }

  private static class ProtoInFn<T extends Message> extends MapFn<ProtobufWritable<T>, T> {
    private final Class<T> clazz;
    private transient ProtobufConverter<T> converter;

    public ProtoInFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.converter = ProtobufConverter.newInstance(clazz);
    }

    @Override
    public T map(ProtobufWritable<T> input) {
      input.setConverter(converter);
      return input.get();
    }
  }

  private static class ProtoOutFn<T extends Message> extends MapFn<T, ProtobufWritable<T>> {
    private final Class<T> clazz;
    private transient ProtobufConverter<T> converter;

    public ProtoOutFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.converter = ProtobufConverter.newInstance(clazz);
    }

    @Override
    public ProtobufWritable<T> map(T input) {
      ProtobufWritable<T> w = new ProtobufWritable<T>();
      w.setConverter(converter);
      w.set(input);
      return w;
    }
  }

  private static class ThriftInFn<T extends TBase<?, ?>> extends MapFn<ThriftWritable<T>, T> {
    private final Class<T> clazz;
    private transient ThriftConverter<T> converter;

    public ThriftInFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.converter = ThriftConverter.newInstance(clazz);
    }

    @Override
    public T map(ThriftWritable<T> input) {
      input.setConverter(converter);
      return input.get();
    }
  }

  private static class ThriftOutFn<T extends TBase<?, ?>> extends MapFn<T, ThriftWritable<T>> {
    private final Class<T> clazz;
    private transient ThriftConverter<T> converter;

    public ThriftOutFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.converter = ThriftConverter.newInstance(clazz);
    }

    @Override
    public ThriftWritable<T> map(T input) {
      ThriftWritable<T> w = new ThriftWritable<T>();
      w.setConverter(converter);
      w.set(input);
      return w;
    }
  }
}
