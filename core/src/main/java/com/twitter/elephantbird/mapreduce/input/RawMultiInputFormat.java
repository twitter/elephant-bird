package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.io.RawBytesWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A {@link MultiInputFormat} that returns the records as raw uninterpreted
 * bytes a {@link BytesWritable}. Converts {@link RawBytesWritable}
 * returned by {@link MultiInputFormat} to BytesWritable. <p>
 *
 * Use MultiInputForamt RawBytesWritable is required or suffices.
 */
@SuppressWarnings("rawtypes")
public class RawMultiInputFormat extends MultiInputFormat {

  @SuppressWarnings("unchecked")
  public RawMultiInputFormat() {
    super(new TypeRef<byte[]>(byte[].class){});
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {
    // use FilterRecord Reader to convert RawBytesWritable to BytesWritable.

    return new FilterRecordReader<LongWritable, Writable>(
        super.createRecordReader(split, taskAttempt)) {

      BytesWritable value = new BytesWritable();

      @Override
      public Writable getCurrentValue() throws IOException, InterruptedException {
        byte[] bytes = ((RawBytesWritable)super.getCurrentValue()).get();
        // TODO extend BytesWritable to avoid extra copy in set().
        value.set(bytes, 0, bytes.length);
        return value;
      }
    };
  }
}
