package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.google.common.base.Preconditions;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * {@link RecordReader} implementation which returns keys and values as {@link DataInputBuffer}
 * instances containing raw bytes. Note that when key or value bytes represent a serialized
 * {@link BinaryWritable} or {@link BytesWritable} instance (or child types such as
 * {@link ThriftWritable}), the first four bytes of the returned buffer will contain run length, as
 * defined by these classes' {@link Writable#write(java.io.DataOutput)} implementation. This 4 byte
 * prefix must be stripped if you're after only the bytes which the original BinaryWritable instance
 * contained.
 *
 * @author Andy Schlaikjer
 */
public class RawSequenceFileRecordReader extends RecordReader<DataInputBuffer, DataInputBuffer> {
  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private final DataOutputBuffer kobuf = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
  private final DataOutputBuffer vobuf = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
  private final DataInputBuffer kibuf = new DataInputBuffer();
  private final DataInputBuffer vibuf = new DataInputBuffer();
  private SequenceFile.Reader reader;
  private ValueBytes vbytes;
  private long start;
  private long end;
  private boolean more, valueUncompressed;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {
    Preconditions.checkNotNull(inputSplit, "InputSplit is null");
    Preconditions.checkNotNull(context, "TaskAttemptContext is null");
    Configuration conf = HadoopCompat.getConfiguration(context);
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path path = fileSplit.getPath();
    // inhibit class loading during SequenceFile.Reader initialization
    reader = new SequenceFile.Reader(path.getFileSystem(conf), path, conf) {
      @Override
      public synchronized Class<?> getKeyClass() {
        return BytesWritable.class;
      }

      @Override
      public synchronized Class<?> getValueClass() {
        return BytesWritable.class;
      }
    };
    vbytes = this.reader.createValueBytes();
    start = fileSplit.getStart();
    if (start > reader.getPosition()) {
      reader.sync(start);
    }
    start = reader.getPosition();
    end = fileSplit.getStart() + fileSplit.getLength();
    more = start < end;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    vbytes = null;
    start = end = 0;
    more = false;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    Preconditions.checkNotNull(reader);
    if (!more) {
      return false;
    }
    long pos = reader.getPosition();
    kobuf.reset();
    int recordLength = reader.nextRaw(kobuf, vbytes);
    if (recordLength < 0 || (pos >= end && reader.syncSeen())) {
      return more = false;
    }
    valueUncompressed = false;
    return true;
  }

  @Override
  public DataInputBuffer getCurrentKey() throws IOException, InterruptedException {
    if (!more) return null;
    kibuf.reset(kobuf.getData(), kobuf.getLength());
    return kibuf;
  }

  @Override
  public DataInputBuffer getCurrentValue() throws IOException, InterruptedException {
    if (!more) return null;
    if (!valueUncompressed) {
      vobuf.reset();
      vbytes.writeUncompressedBytes(vobuf);
      valueUncompressed = true;
    }
    vibuf.reset(vobuf.getData(), vobuf.getLength());
    return vibuf;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (reader.getPosition() - start) / (float) (end - start));
    }
  }
}
