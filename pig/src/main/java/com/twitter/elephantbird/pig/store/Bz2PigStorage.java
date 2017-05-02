package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;

/**
 * Enables bzip2 compression for storage.<br>
 * This is similar to:
 * <pre>
 *   set output.compression.enabled true;
 *   set output.compression.codec org.apache.hadoop.io.compress.BZip2Codec;
 *   storage alias using PigStorage();
 * </pre>
 */
public class Bz2PigStorage extends PigStorage {
  // Ideally, PigStorage it self should take more options like compression
  // codec etc.
  public Bz2PigStorage() {
    super();
  }

  public Bz2PigStorage(String delimiter) {
    super(delimiter);
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    HadoopCompat.getConfiguration(job).set("output.compression.enabled", "true");
    HadoopCompat.getConfiguration(job).set("output.compression.codec", BZip2Codec.class.getName());
    super.setStoreLocation(location, job);
  }

}
