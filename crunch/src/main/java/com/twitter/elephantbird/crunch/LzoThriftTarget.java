package com.twitter.elephantbird.crunch;

import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TBase;

/**
 * A Crunch {@code Target} for writing records to an {@link LzoThriftBlockOutputFormat} file.
 */
public class LzoThriftTarget extends FileTargetImpl {
  private FormatBundle<LzoThriftBlockOutputFormat> bundle;

  public LzoThriftTarget(Path path) {
    super(path, LzoThriftBlockOutputFormat.class, SequentialFileNamingScheme.getInstance());
    this.bundle = FormatBundle.forOutput(LzoThriftBlockOutputFormat.class);
  }

  @Override
  public LzoThriftTarget outputConf(String key, String value) {
    super.outputConf(key, value);
    bundle.set(key, value);
    return this;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (TBase.class.isAssignableFrom(ptype.getTypeClass())) {
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
        "elephantbird.protobuf.class.for." + LzoThriftBlockOutputFormat.class.getName(),
        ptype.getTypeClass().getName());
    configureForMapReduce(job, keyClass, valueClass, bundle, outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (TBase.class.isAssignableFrom(ptype.getTypeClass())) {
      return new LzoThriftSourceTarget(path, ptype);
    } else {
      return null;
    }
  }
}
