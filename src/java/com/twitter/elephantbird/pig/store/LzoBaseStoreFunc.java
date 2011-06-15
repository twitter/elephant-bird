package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.FuncSpec;
import org.apache.pig.StoreFunc;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * This class serves as the base class for any functions storing output via lzo.
 * It implements the common functions it can, and wraps the given output stream in an lzop
 * output stream.
 */
public abstract class LzoBaseStoreFunc extends StoreFunc {

  @SuppressWarnings("rawtypes")
  protected RecordWriter writer = null;

  @Override
  public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) {
    this.writer = writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    job.getConfiguration().set("mapred.textoutputformat.separator", "");
    FileOutputFormat.setOutputPath(job, new Path(location));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
  }
}
