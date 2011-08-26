package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * {@link RecordReader} implementation which returns keys and values as {@link DataInputBuffer}
 * instances.
 *
 * @author Andy Schlaikjer
 */
public class RawSequenceFileRecordReader extends RecordReader<DataInputBuffer, DataInputBuffer> {
  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private final DataOutputBuffer kobuf = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
  private final DataOutputBuffer vobuf = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
  private final DataInputBuffer kibuf = new DataInputBuffer();
  private final DataInputBuffer vibuf = new DataInputBuffer();
  private FileSplit fileSplit;
  private SequenceFile.Reader reader;
  private ValueBytes vbytes;
  private boolean more, valueUncompressed;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {
    Preconditions.checkNotNull(inputSplit, "InputSplit is null");
    Preconditions.checkNotNull(context, "TaskAttemptContext is null");
    Configuration conf = context.getConfiguration();
    this.fileSplit = (FileSplit) inputSplit;
    Path path = this.fileSplit.getPath();
    this.reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
    this.vbytes = this.reader.createValueBytes();
    this.more = true;
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkNotNull(reader);
    reader.close();
    reader = null;
    more = false;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    Preconditions.checkNotNull(reader);
    Preconditions.checkState(more);
    kobuf.reset();
    int recordLength = reader.nextRaw(kobuf, vbytes);
    if (recordLength < 0) {
      return more = false;
    }
    valueUncompressed = false;
    return true;
  }

  @Override
  public DataInputBuffer getCurrentKey() throws IOException, InterruptedException {
    Preconditions.checkState(more);
    kibuf.reset(kobuf.getData(), kobuf.getLength());
    return kibuf;
  }

  @Override
  public DataInputBuffer getCurrentValue() throws IOException, InterruptedException {
    Preconditions.checkState(more);
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
    Preconditions.checkNotNull(reader);
    Preconditions.checkNotNull(fileSplit);
    return ((float) reader.getPosition()) / fileSplit.getLength();
  }
}
