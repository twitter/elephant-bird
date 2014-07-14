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
  private final Queue<DelayedRecordReader> recordReaders = new LinkedList<DelayedRecordReader>();
  private RecordReader<K, V> currentRecordReader;
  // The key and value objects are necessary for mapred interop via MapredInputFormatCompatible. The
  // DeprecatedInputFormatWrapper ensures that the same objects are used to ferry data around, and so we
  // must cache these locally as when RecordReaders roll over, we need to make sure the new ones are using
  // the same objects.
  private K key;
  private V value;
  private int recordReadersCount = 0;
  private int currentRecordReaderIndex = -1;
  private long totalSplitLengths = 0;
  private long[] cumulativeSplitLengths;
  private long[] splitLengths;
  // Set to true after we've initialized the first delegate RecordRecord and have set the key and value objects
  // based on that.
  private boolean haveInitializedFirstRecordReader = false;

  public CompositeRecordReader(InputFormat<K, V> delegate) {
    this.delegate = delegate;
  }

  /**
   * In order to avoid opening all of the file handles at once (and before they are actually necessary), we
   * wait until the RecordReader is actually used in order to initialize it.
   */
  private class DelayedRecordReader {
    private InputSplit inputSplit;
    private TaskAttemptContext taskAttemptContext;

    public DelayedRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
      this.inputSplit = inputSplit;
      this.taskAttemptContext = taskAttemptContext;
    }

    public RecordReader<K, V> createRecordReader() throws IOException, InterruptedException {
      RecordReader<K, V> reader = delegate.createRecordReader(inputSplit, taskAttemptContext);
      if (!(reader instanceof MapredInputFormatCompatible)) {
        throw new RuntimeException("RecordReader does not implement MapredInputFormatCompatible. " +
                "Received: " + reader);
      }
      reader.initialize(inputSplit, taskAttemptContext);
      return reader;
    }
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (!(inputSplit instanceof CompositeInputSplit)) {
      throw new IOException("InputSplit must be a CompositeInputSplit. Received: " + inputSplit);
    }
    List<InputSplit> splits = ((CompositeInputSplit) inputSplit).getSplits();
    int numSplits = splits.size();
    cumulativeSplitLengths = new long[numSplits];
    splitLengths = new long[numSplits];
    long localTotalSplitLength = 0;
    for (int i = 0; i < numSplits; i++) {
      InputSplit split = splits.get(i);
      recordReaders.add(new DelayedRecordReader(split, taskAttemptContext));
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
    // This is essentially tail recursion, but java style
    while (true) {
      // No record reader, and there are no more record readers. No more KV's
      if (currentRecordReader == null && recordReaders.isEmpty()) {
        return false;
      } else if (currentRecordReader != null) {
        // We have a record reader, and it is either done, or we have more values.
        if (currentRecordReader.nextKeyValue()) {
          return true;
        }
        currentRecordReader.close();
        // Rely on the rest of the loop to get a next currentRecordReader, if there is one available.
        currentRecordReader = null;
      }

      // At this point, there is no currentRecordReader and no more recordReaders. No more KVs.
      if (recordReaders.isEmpty()) {
        return false;
      }
      currentRecordReader = recordReaders.remove().createRecordReader();
      currentRecordReaderIndex++;
      // The DeprecatedInputFormatWrapper has a check in it which ensures that the key and value
      // objects of the underlying MapredInputFormatCompatible are the same. Thus, we rely on the
      // very first RecordReader that we instantiate to create the underlying objects which we
      // will use for the rest of the process.
      if (!haveInitializedFirstRecordReader) {
        key = currentRecordReader.getCurrentKey();
        value = currentRecordReader.getCurrentValue();
        haveInitializedFirstRecordReader = true;
      } else {
        // This call is purely for interop with DeprecatedInputFormatWrapper. It ensures that a pair of
        // key value objects which were set by a calling function are passed to the new delegate so that it
        // will be consistent the entire time.
        setKeyValue(key, value);
      }
      // We will loop again and see if there is a nextKeyValue
    }
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
      return 1f;
    }

    if (totalSplitLengths == 0) {
      return 0f;
    }

    long cur = currentRecordReader == null ?
      0L : (long)(currentRecordReader.getProgress() * splitLengths[currentRecordReaderIndex]);
    return 1.0f * (cur + cumulativeSplitLengths[currentRecordReaderIndex]) / totalSplitLengths;
  }

  @Override
  public void close() throws IOException {
    if (currentRecordReader != null) {
      currentRecordReader.close();
    }
  }

  @Override
  public void setKeyValue(K key, V value) {
    ((MapredInputFormatCompatible) currentRecordReader).setKeyValue(key, value);
    this.key = key;
    this.value = value;
  }
}
