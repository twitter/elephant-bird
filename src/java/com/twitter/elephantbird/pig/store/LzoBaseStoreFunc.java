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

  protected FuncSpec storeFuncSpec_;

  @Override
  public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) {
    this.writer = writer;
  }

  /**
   * Set the storage spec so any arguments given in the script are tracked, to be reinstantiated by the mappers.
   * @param clazz the class of the load function to use.
   * @param args an array of strings that are fed to the class's constructor.
   */
  protected void setStorageSpec(Class <? extends LzoBaseStoreFunc> clazz, String[] args) {
    storeFuncSpec_ = new FuncSpec(clazz.getName(), args);
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    job.getConfiguration().set("mapred.textoutputformat.separator", "");
    FileOutputFormat.setOutputPath(job, new Path(location));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
  }
}
