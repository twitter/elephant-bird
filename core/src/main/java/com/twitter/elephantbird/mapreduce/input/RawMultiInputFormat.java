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
 * A {@link MultiInputFormat} that returns records as uninterpreted
 * {@link BytesWritable}. Converts {@link RawBytesWritable}
 * returned by {@link MultiInputFormat} to a BytesWritable. <p>
 *
 * Use MultiInputFormat if RawBytesWritable is required or suffices.
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

      // extend BytesWritable to avoid a copy.
      byte[] bytes;
      BytesWritable value = new BytesWritable() {
        public byte[] getBytes() {
          return bytes;
        }

        public int getLength() {
          return bytes.length;
        }
      };

      @Override
      public Writable getCurrentValue() throws IOException, InterruptedException {
        bytes = ((RawBytesWritable)super.getCurrentValue()).get();
        return value;
      }
    };
  }
}
