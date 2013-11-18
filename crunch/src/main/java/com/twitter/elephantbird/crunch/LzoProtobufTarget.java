package com.twitter.elephantbird.crunch;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * A Crunch {@code Target} for writing messages to an {@link LzoProtobufBlockOutputFormat} file.
 */
public class LzoProtobufTarget extends FileTargetImpl {

  private FormatBundle<LzoProtobufBlockOutputFormat> bundle;

  public LzoProtobufTarget(Path path) {
    super(path, LzoProtobufBlockOutputFormat.class, SequentialFileNamingScheme.getInstance());
    this.bundle = FormatBundle.forOutput(LzoProtobufBlockOutputFormat.class);
  }

  @Override
  public LzoProtobufTarget outputConf(String key, String value) {
    super.outputConf(key, value);
    bundle.set(key, value);
    return this;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (Message.class.isAssignableFrom(ptype.getTypeClass())) {
      handler.configure(this, ptype);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    Converter converter = getConverter(ptype);
    Class keyClass = converter.getKeyClass();
    Class valueClass = converter.getValueClass();
    bundle.set(
        "elephantbird.protobuf.class.for." + LzoProtobufBlockOutputFormat.class.getName(),
        ptype.getTypeClass().getName());
    configureForMapReduce(job, keyClass, valueClass, bundle, outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (Message.class.isAssignableFrom(ptype.getTypeClass())) {
      return new LzoProtobufSourceTarget(path, ptype);
    } else {
      return null;
    }
  }
}
