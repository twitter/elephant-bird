package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * A wrapper for {@link PigStorage} that forces LZO compression for
 * storing. LzoTextInputFormat is used for loading since PigStorage
 * can not split lzo files.
 *
 * This is similar to:
 * <pre>
 *   set output.compression.enabled true;
 *   set output.compression.codec com.hadoop.compression.lzo.LzopCodec;
 *   storage alias using PigStorage();
 * </pre>
 */
public class LzoPigStorage extends PigStorage {

  public LzoPigStorage() {
    super();
  }

  public LzoPigStorage(String delimiter) {
    super(delimiter);
  }

  @Override
  public InputFormat<LongWritable, Text> getInputFormat() {
    // PigStorage can handle lzo files, but cannot split them.
    return new LzoTextInputFormat();
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    // looks like each context gets different job. modifying conf here
    // does not affect subsequent store statements in the script.
    job.getConfiguration().set("output.compression.enabled", "true");
    job.getConfiguration().set("output.compression.code", LzopCodec.class.getName());
    super.setStoreLocation(location, job);
  }
}
