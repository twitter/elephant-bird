package com.twitter.elephantbird.pig.store;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import com.twitter.elephantbird.mapreduce.output.LuceneIndexOutputFormat;

/**
 * A StoreFunc that writes lucene indexes by wrapping a
 * {@link PigLuceneIndexOutputFormat}
 * <p>
 * Usage:
 * <code>
 * store x into '/some/path'
 * using LuceneIndexStorage('com.example.MyPigLuceneIndexOutputFormat');
 * </code>
 *
 * @author Alex Levenson
 */
public class LuceneIndexStorage extends StoreFunc {
  private PigLuceneIndexOutputFormat outputFormat;
  private RecordWriter<NullWritable, Tuple> recordWriter;

  /**
   * Used for instantiation from a subclass
   *
   * @param outputFormatClass output format to delegate to
   */
  protected LuceneIndexStorage(Class<? extends PigLuceneIndexOutputFormat>
                               outputFormatClass) {
    try {
      outputFormat = outputFormatClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Used for instantiation from a pig script
   *
   * @param outputFormatClass output format to delegate to
   */
  public LuceneIndexStorage(String outputFormatClass) {
    try {
      outputFormat = (PigLuceneIndexOutputFormat)
         Class.forName(outputFormatClass).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputFormat getOutputFormat() {
    return outputFormat;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    LuceneIndexOutputFormat.setOutputPath(job, new Path(location));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void prepareToWrite(RecordWriter r) throws IOException {
    this.recordWriter = r;
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {
    try {
      this.recordWriter.write(NullWritable.get(), tuple);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  /**
   * This helper base class lets us load {@link LuceneIndexOutputFormat} reflectively
   * without unsafe casts.
   */
  public static abstract class PigLuceneIndexOutputFormat
    extends LuceneIndexOutputFormat<NullWritable, Tuple>  { }
}
