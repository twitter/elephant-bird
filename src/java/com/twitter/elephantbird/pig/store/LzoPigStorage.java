package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * A wrapper for {@link PigStorage} that forces LZO compression for
 * storing. Loading LZO files is supported natively by PIG as long
 * as lzo codec is configured.
 *
 * This is equivalent to:
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
  public void setStoreLocation(String location, Job job) throws IOException {
    // looks like each context gets different job. modifying conf here
    // does not affect subsequent store statements in the script.
    job.getConfiguration().set("output.compression.enabled", "true");
    job.getConfiguration().set("output.compression.code", LzopCodec.class.getName());
    super.setStoreLocation(location, job);
  }
}
