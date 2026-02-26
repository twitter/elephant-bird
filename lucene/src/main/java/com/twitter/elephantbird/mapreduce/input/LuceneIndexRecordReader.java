package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat.LuceneIndexInputSplit;

/**
 * See {@link LuceneIndexInputFormat}
 *
 * @param <T> - the type that your lucene Documents will be converted to
 * @author Alex Levenson
 */
public abstract class LuceneIndexRecordReader<T extends Writable>
    extends RecordReader<IntWritable, T> {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LuceneIndexRecordReader.class);

  private IntWritable currentKey;
  private T currentValue;
  private Iterator<T> currentValueIter;

  private List<Query> queries;
  private ListIterator<Query> currentQueryIter;

  private ListIterator<Path> currentIndexPathIter;
  private IndexReader currentIndexReader;
  private int numIndexes;

  private IndexSearcher indexSearcher;
  private Configuration conf;

  protected TaskAttemptContext context;

  /**
   * Convert a query from its serialized (string) form to a {@link Query}
   * This can be done using a QueryParser if your serialized form is a lucene query string,
   * or with custom {@link Query} serialization / deserialization
   *
   * @param serializedQuery string representation of the query as retrieved from the job conf
   * @return a {@link Query} to use to search the index
   * @throws IOException
   */
  protected abstract Query deserializeQuery(String serializedQuery) throws IOException;

  /**
   * Open an index stored in path
   * Override if you want more control over how indexes are opened
   *
   * @param path path to the index
   * @param conf job conf
   * @return an IndexReader of the index in path
   * @throws IOException
   */
  protected IndexReader openIndex(Path path, Configuration conf) throws IOException {
    return DirectoryReader.open(new LuceneHdfsDirectory(path, path.getFileSystem(conf)));
  }

  /**
   * Given an {@link IndexReader} return an {@link IndexSearcher} for it
   * Override if you want more control over how the {@link IndexSearcher} is created
   * @param reader the IndexReader
   * @return an IndexSearcher
   */
  protected IndexSearcher createSearcher(IndexReader reader) {
    return new IndexSearcher(reader);
  }

  /**
   * Search the index and return an Iterator of values extracted from the index.
   * This should probably be a lazy iterator if there will be many returned values.
   *
   * @param searcher the index searcher to query
   * @param query the query to run
   * @return an iterator of values to be emitted as records (one by one) by this record reader
   * @throws IOException
   */
  protected abstract Iterator<T> search(final IndexSearcher searcher, Query query)
      throws IOException;

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {

    conf = HadoopCompat.getConfiguration(context);
    this.context = context;

    // convert query strings in the job conf to lucene Query objects
    List<String> serializedQueries = LuceneIndexInputFormat.getQueries(conf);
    queries = Lists.newArrayListWithCapacity(serializedQueries.size());
    for (String serializedQuery : serializedQueries) {
      queries.add(deserializeQuery(serializedQuery));
    }

    // get the indexes this record reader needs to read
    List<Path> indexes = ((LuceneIndexInputSplit) genericSplit).getIndexDirs();
    numIndexes = indexes.size();
    // we will iterate over this just once
    currentIndexPathIter = indexes.listIterator();

    // set the value and query iterators to empty so that
    // nextKeyValue() will roll over to the next index
    currentValueIter = Collections.<T>emptyList().listIterator();
    currentQueryIter = Collections.<Query>emptyList().listIterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // first exhaust currentValues
    if (currentValueIter.hasNext()) {
      currentValue = currentValueIter.next();
      return true;
    }

    // no more currentValues, move to the next query
    // and try again
    if (currentQueryIter.hasNext()) {
      currentKey = new IntWritable(currentQueryIter.nextIndex());
      Query nextQuery = currentQueryIter.next();
      LOG.info("Running query " + nextQuery);
      Iterator<T> values = search(indexSearcher, nextQuery);
      currentValueIter = values;
      return nextKeyValue();
    }

    // no more currentValues nor queries in this index, move to the next index
    if (currentIndexPathIter.hasNext()) {
      closeIndexReader(currentIndexReader);
      Path next = currentIndexPathIter.next();
      LOG.info("Searching index: " + next);
      currentIndexReader = openIndex(next, conf);
      indexSearcher = createSearcher(currentIndexReader);
      currentQueryIter = queries.listIterator();
      return nextKeyValue();
    }

    // all done
    return false;
  }

  /**
   * Because {@link IndexReader#close()} is final, it is impossible to properly mock an IndexReader
   * in unit tests. Wrapping the call to close in this method allows us to override this method
   * during tests.
   *
   * NOTE: There's no good reason to override this method (unless you're creating mock IndexReaders)
   *
   * @param reader to close
   * @throws IOException
   */
  protected void closeIndexReader(IndexReader reader) throws IOException {
    Closeables.close(currentIndexReader, false);
  }

  @Override
  public IntWritable getCurrentKey() throws IOException, InterruptedException {
    return currentKey;
  }

  @Override
  public T getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }

  /**
   * This is sort of an approximation of progress.
   * It splits the progress equally among all indexes, then among all queries for that index.
   * So it won't move linearly, since we don't know how many hits there will be per query
   */
  @Override
  public float getProgress() {
    if (numIndexes < 1) {
      return 1.0f;
    }

    float indexProgress = (float) currentIndexPathIter.previousIndex() / (float) numIndexes;

    float queriesProgress = 1.0f;
    if (queries.size() > 0) {
      queriesProgress = (float) currentQueryIter.previousIndex() / (float) queries.size();
    }

    queriesProgress *= 1.0f / numIndexes;

    return indexProgress + queriesProgress;
  }
}
