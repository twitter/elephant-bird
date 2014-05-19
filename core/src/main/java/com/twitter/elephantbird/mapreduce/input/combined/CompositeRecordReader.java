package com.twitter.elephantbird.mapreduce.input.combined;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
 * Created with IntelliJ IDEA.
 * User: jcoveney
 * Date: 5/16/14
 * Time: 12:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class CompositeRecordReader<K, V> extends RecordReader<K, V>
        implements MapredInputFormatCompatible<K, V>  {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeRecordReader.class); //TODO remove

  private final InputFormat<K, V> delegate;
  private final Queue<RecordReader<K, V>> recordReaders = new LinkedList<RecordReader<K, V>>();
  private RecordReader<K, V> currentRecordReader;
  private K key;
  private V value;

  public CompositeRecordReader(InputFormat<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (!(inputSplit instanceof CompositeInputSplit)) {
      throw new IOException("InputSplit must be a CompositeInputSplit. Received: " + inputSplit);
    }
    for (InputSplit split : ((CompositeInputSplit) inputSplit).getSplits()) {
      RecordReader<K, V> recordReader = delegate.createRecordReader(split, taskAttemptContext);
      if (!(recordReader instanceof MapredInputFormatCompatible)) {
        throw new RuntimeException("RecordReader does not implement MapredInputFormatCompatible. " +
                "Received: " + recordReader);
      }
      recordReader.initialize(split, taskAttemptContext);
      recordReaders.add(recordReader);
      LOG.info("Created record reader for split: " + split + "\nRecord reader: " + recordReader); //TODO remove
    }
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
    return 0; //TODO need to think about this
  }

  @Override
  public void close() throws IOException {
    if (currentRecordReader != null) {
      currentRecordReader.close();
    }
    for (RecordReader<K, V> recordReader : recordReaders) {
      recordReader.close();
    }
  }

  @Override
  public void setKeyValue(K key, V value) {
    ((MapredInputFormatCompatible) currentRecordReader).setKeyValue(key, value);
    this.key = key;
    this.value = value;
  }
}
