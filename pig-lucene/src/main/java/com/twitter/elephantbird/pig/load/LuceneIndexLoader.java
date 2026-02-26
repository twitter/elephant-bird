package com.twitter.elephantbird.pig.load;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat;
import com.twitter.elephantbird.mapreduce.input.LuceneIndexRecordReader;
import com.twitter.elephantbird.util.HdfsUtils;

/**
 * Base class for LoadFuncs that load data from lucene indexes.
 * <p>
*  Wraps {@link LuceneIndexInputFormat}
 * <p>
 * Subclasses must provide a {@link LuceneIndexInputFormat} and specify how to convert an MR
 * record to a tuple
 * <p>
 * Constructor has two formats, one for loading queries from a file and one for supplying them
 * directly as pig string literals.
 * For example:
 * <code>
 * x = load '/some/path' using MyLuceneIndexLoader('--queries', 'a query', 'another query');
 * </code>
 * or
 * <code>
 * x = load '/some/path' using MyLuceneIndexLoader('--file', 'path/to/local/file');
 *</code>
 *
 * The file should have one query per line and be UTF-8 encoded
 * In both cases, the strings provided (as literals or in a file) are the serialized form
 * of the query used by {@link LuceneIndexInputFormat#setQueries(List, Configuration)}
 *
 * @param <T> type of records that will be converted to tuples
 * @author Alex Levenson
 */
public abstract class LuceneIndexLoader<T extends Writable> extends LoadFunc {

  private static final String USAGE_HELP = "LuceneIndexLoader's constructor usage:\n"
    + "LuceneIndexLoader('--queries', 'a query', 'another query')\nor\n"
    + "LuceneIndexLoader('--file', 'path/to/local/file')";

  private LuceneIndexRecordReader<T> reader;
  protected List<String> queries = null;
  protected File queryFile = null;

  /**
   * Convert a value from the InputFormat to a tuple
   *
   * @param key the id of the the query this record belongs to
   * @param value the value from the input format
   * @return a Tuple representing this key value pair (can have whatever schema you'd like)
   */
  protected abstract Tuple recordToTuple(int key, T value);

  /**
   * Provide an instance of the {@link LuceneIndexInputFormat} to be wrapped by this loader
   * @return an instance of the {@link LuceneIndexInputFormat} to be wrapped by this loader
   * @throws IOException
   */
  protected abstract LuceneIndexInputFormat<T> getLuceneIndexInputFormat() throws IOException;

  public LuceneIndexLoader(String[] args) {
    Preconditions.checkNotNull(args, USAGE_HELP);
    Preconditions.checkArgument(args.length >= 2, USAGE_HELP);
    Preconditions.checkNotNull(args[0], USAGE_HELP);

    if (args[0].equals("--queries")) {
      queries = Lists.newArrayList(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].equals("--file")) {
      Preconditions.checkArgument(args.length == 2, USAGE_HELP);
      queryFile = new File(args[1]);
    } else {
      throw new IllegalArgumentException(USAGE_HELP);
    }
  }

  /**
   * Subclasses may use this constructor, but they will have to set queries or queryFile
   * themselves. Failure to do so will cause an IllegalArgumentException in setLocation
   */
  protected LuceneIndexLoader() { }

  /**
   * THIS INVOLVES AN UNCHECKED CAST
   * Pig gives us a raw type for the RecordReader unfortunately.
   * However, because {@link #getInputFormat()} is final and delegates
   * to {@link #getLuceneIndexInputFormat()} we can be reasonably sure
   * that this record reader is actually a LuceneIndexRecordReader<T>
   */
  @Override
  @SuppressWarnings("unchecked")
  public void prepareToRead(RecordReader reader, PigSplit split) {
    this.reader = (LuceneIndexRecordReader<T>) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    Preconditions.checkArgument(queries != null || queryFile != null,
        "Either queires or queryFile must be set in the constructor!");

    Configuration conf = HadoopCompat.getConfiguration(job);
    // prevent pig from trying to combine splits, let LuceneIndexInputFormat do that
    conf.setBoolean("pig.noSplitCombination", true);

    // lazy load the queries into conf
    if (!LuceneIndexInputFormat.queriesSet(conf)) {
      if (queries != null) {
        LuceneIndexInputFormat.setQueries(queries, conf);
      } else {
        LuceneIndexInputFormat.setQueries(loadQueriesFromFile(conf), conf);
      }
    }

    // a little bit of logic to support comma separated locations and also
    // hdfs glob syntax locations
    String[] locationsWithGlobs = getPathStrings(location);
    List<Path> expandedPaths = HdfsUtils.expandGlobs(Arrays.asList(locationsWithGlobs), conf);
    LuceneIndexInputFormat.setInputPaths(expandedPaths, conf);
  }

  protected List<String> loadQueriesFromFile(Configuration conf) throws IOException {
    Preconditions.checkArgument(queryFile.exists(),
      "Query file: " + queryFile + " does not exist!");
    List<String> lines = Files.readLines(queryFile, Charsets.UTF_8);
    List<String> strippedLines = Lists.newArrayListWithCapacity(lines.size());
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty()) {
        strippedLines.add(line);
      }
    }
    return strippedLines;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!reader.nextKeyValue()) {
        // all done
        return null;
      }

      int key;
      T value;

      try {
        key = reader.getCurrentKey().get();
        value = reader.getCurrentValue();
      } catch (ClassCastException e) {
        throw new IOException("Record reader did not return correct key or value type", e);
      }

      return recordToTuple(key, value);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  // enforce some more type safety than pig gives us by default
  @Override
  public final InputFormat getInputFormat() throws IOException {
    return getLuceneIndexInputFormat();
  }

}
