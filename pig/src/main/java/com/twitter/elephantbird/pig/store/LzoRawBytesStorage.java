package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.twitter.elephantbird.mapreduce.io.RawBytesWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryBlockOutputFormat;
import com.twitter.elephantbird.pig.load.LzoBaseLoadFunc;
import com.twitter.elephantbird.pig.load.LzoRawBytesLoader;

/**
 * Stores raw byte[] records with LZO block-compression, suitable for reading via
 * {@link LzoRawBytesLoader} or some other {@link LzoBaseLoadFunc}.
 *
 * @author Andy Schlaikjer
 */
public class LzoRawBytesStorage extends BaseStoreFunc {
  private final RawBytesWritable writable = new RawBytesWritable();

  @Override
  @SuppressWarnings("rawtypes")
  public OutputFormat getOutputFormat() throws IOException {
    return new LzoBinaryBlockOutputFormat();
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    DataByteArray data = null;
    if (t == null || t.size() < 1 || (data = (DataByteArray) t.get(0)) == null) {
      // TODO(Andy Schlaikjer): Signal error
      return;
    }
    writable.set(data.get());
    writeRecord(null, writable);
  }
}
