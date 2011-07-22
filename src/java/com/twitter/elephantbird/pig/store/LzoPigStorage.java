package com.twitter.elephantbird.pig.store;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import com.twitter.elephantbird.mapreduce.output.LzoOutputFormat;

/**
 * A wrapper for {@link PigStorage} to enable LZO compression.
 * LzoTextInputFormat is used for loading since PigStorage
 * can not split lzo files.
 * LzoTextOutputFormat is used for storage so that lzo index files
 * can be written at the same time.
 *
 * This is similar to:
 * <pre>
 *   set output.compression.enabled true;
 *   set output.compression.codec com.hadoop.compression.lzo.LzopCodec;
 *   store/load ... using PigStorage();
 * </pre>
 */
public class LzoPigStorage extends PigStorage {

  private String delimiter = null; // temporary for outpupt format

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
  public OutputFormat<NullWritable, Tuple> getOutputFormat() {
    // LzoOutputFormat can write lzo index file.
    // LzoTextInputFormat can't be used here.
    return new TupleOutputFormat(delimiter);
  }

  // This is a temporary work around for PigStorage since
  // it writes a Tuple to outputformat rather than Text.
  // This may change soon and we can use LzoTextOutputFormat directly.
  protected static class TupleOutputFormat extends LzoOutputFormat<NullWritable, Tuple> {

    private byte fieldDel;

    public TupleOutputFormat(String delimiter) {
      this.fieldDel = delimiter == null ? (byte)'\t' : StorageUtil.parseFieldDel(delimiter);
    }

    @Override
    public RecordWriter<NullWritable, Tuple> getRecordWriter(
        TaskAttemptContext job) throws IOException, InterruptedException {
      final DataOutputStream out = getOutputStream(job);

      return new RecordWriter<NullWritable, Tuple>() {
        public void close(TaskAttemptContext context) throws IOException,
                                                      InterruptedException {
          out.close();
        }

        public void write(NullWritable key, Tuple value) throws IOException,
                                                         InterruptedException {
          int sz = value.size();
          for (int i = 0; i < sz; i++) {
              StorageUtil.putField(out, value.get(i));
              if (i != sz - 1) {
                  out.writeByte(fieldDel);
              }
          }
          out.write('\n');
        }
      };
    }
  }
}
