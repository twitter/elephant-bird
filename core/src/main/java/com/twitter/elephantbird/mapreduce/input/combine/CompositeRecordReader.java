package com.twitter.elephantbird.mapreduce.input.combine;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * This is a record reader which delegates to the RecordReader of a delegate
 * InputSplit and manages those RecordReaders over all of the splits in a
 * CompositeInputSplit. It is not as general as it could be as it is meant
 * to work with {@link com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper},
 * which means that input RecordReaders must implement
 * {@link MapredInputFormatCompatible} for compatibility with the mapred
 * interface.
 *
 * @author Jonathan Coveney
 */
public class CompositeRecordReader<K, V> extends RecordReader<K, V>
        implements MapredInputFormatCompatible<K, V>  {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeRecordReader.class);

  private final InputFormat<K, V> delegate;
  private final Queue<RecordReader<K, V>> recordReaders = new LinkedList<RecordReader<K, V>>();
  private RecordReader<K, V> currentRecordReader;
  private K key;
  private V value;
  private int recordReadersCount = 0;
  private int currentRecordReaderIndex = 0;
  private float totalSplitLengths = 0;
  private float[] cumulativeSplitLengths;
  private float[] splitLengths;

  public CompositeRecordReader(InputFormat<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (!(inputSplit instanceof CompositeInputSplit)) {
      throw new IOException("InputSplit must be a CompositeInputSplit. Received: " + inputSplit);
    }
    List<InputSplit> splits = ((CompositeInputSplit) inputSplit).getSplits();
    int numSplits = splits.size();
    cumulativeSplitLengths = new float[numSplits];
    splitLengths = new float[numSplits];
    long localTotalSplitLength = 0;
    for (int i = 0; i < numSplits; i++) {
      InputSplit split = splits.get(i);
      RecordReader<K, V> recordReader = delegate.createRecordReader(split, taskAttemptContext);
      if (!(recordReader instanceof MapredInputFormatCompatible)) {
        throw new RuntimeException("RecordReader does not implement MapredInputFormatCompatible. " +
                "Received: " + recordReader);
      }
      recordReader.initialize(split, taskAttemptContext);
      recordReaders.add(recordReader);
      long splitLength = split.getLength();
      splitLengths[i] = splitLength;
      cumulativeSplitLengths[i] = localTotalSplitLength;
      localTotalSplitLength += splitLength;
    }
    totalSplitLengths = localTotalSplitLength;
    recordReadersCount = recordReaders.size();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currentRecordReader == null) {
      if (recordReaders.isEmpty()) {
        return false;
      }
      currentRecordReader = recordReaders.remove();
    }
    while (!currentRecordReader.nextKeyValue()) {
      if (recordReaders.isEmpty()) {
        return false;
      }
      currentRecordReader = recordReaders.remove();
      currentRecordReaderIndex++;
      setKeyValue(key, value);
    }
    return true;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return currentRecordReader.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return currentRecordReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (recordReadersCount < 1) {
      return 1.0f;
    }

    return (currentRecordReader.getProgress() / splitLengths[currentRecordReaderIndex])
      + (cumulativeSplitLengths[currentRecordReaderIndex] / totalSplitLengths);
  }

  @Override
  public void close() throws IOException {
    if (currentRecordReader != null) {
      currentRecordReader.close();
    }
    Exception firstException = null;
    for (RecordReader<K, V> recordReader : recordReaders) {
      try {
        recordReader.close();
      } catch (Exception e) {
        LOG.error("Exception while closing RecordReader", e);
        if (firstException == null) {
          firstException = e;
        }
      }
    }
    if (firstException != null) {
      if (firstException instanceof IOException) {
        throw (IOException) firstException;
      } else {
        throw new IOException("Exception when closing RecordReader", firstException);
      }
    }
  }

  @Override
  public void setKeyValue(K key, V value) {
    ((MapredInputFormatCompatible) currentRecordReader).setKeyValue(key, value);
    this.key = key;
    this.value = value;
  }
}
