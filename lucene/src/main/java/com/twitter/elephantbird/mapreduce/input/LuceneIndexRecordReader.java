package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat.LuceneIndexInputSplit;

/**
 * See {@link LuceneIndexInputFormat}
 *
 * @param <T> - the type that your lucene Documents will be converted to
 * @author Alex Levenson
 */
public abstract class LuceneIndexRecordReader<T extends Writable>
    extends RecordReader<IntWritable, T> {

  private IntWritable currentKey;
  private T currentValue;
  private Iterator<T> currentValueIter;
  private float currentValueIterSize;
  private float currentValueIterPosition;

  private List<Query> queries;
  private ListIterator<Query> currentQueryIter;

  private ListIterator<Path> currentIndexPathIter;
  private int numIndexes;

  private IndexSearcher indexSearcher;
  private Configuration conf;

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
    return DirectoryReader.open(new HdfsDirectory(path, path.getFileSystem(conf)));
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
   * Search the index and return a list of values extracted from the index
   * The default implementation applies {@link #docToValue(Document)} to every {@link ScoreDoc}
   * found by executing query over searcher
   *
   * You can override this to control how the searching is done and what is returned. For example
   * you could return only the top N hits for the query, or even just the number of hits
   *
   * Although the return value is a {@link List}, it can be a lazy list as used in the default
   * implementation. The return value will only be iterated over once.
   *
   * @param searcher the index searcher to query
   * @param query the query to run
   * @return a list of values to be emitted as records (one by one) by this record reader
   * @throws IOException
   */
  protected List<T> search(final IndexSearcher searcher, Query query) throws IOException {
    TopDocs topDocs = searcher.search(query, Integer.MAX_VALUE);
    return Lists.transform(Arrays.asList(topDocs.scoreDocs),
      new Function<ScoreDoc, T>() {
        @Override
        public T apply(ScoreDoc scoreDoc) {
          try {
            return docToValue(searcher.doc(scoreDoc.doc));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
  }

  /**
   * Convert a {@link Document} to a value to be emitted by this record reader
   * This is called by the default implementation of {@link #search}
   * If you do not override {@link #search} you must implement this method.
   * If you do override {@link #search} then you may or may not implement this method
   * depending on whether you choose to use it
   *
   * TODO: pretty ugly to have a required but sometimes optional method, maybe move to a sublcass
   * TODO: like AllHitsRecordReader?
   *
   * @param doc document to convert
   * @return a value to be emitted from this record reader
   */
  protected abstract T docToValue(Document doc);

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {

    conf = context.getConfiguration();

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
      currentValueIterPosition++;
      return true;
    }

    // no more currentValues, move to the next query
    // and try again
    if (currentQueryIter.hasNext()) {
      currentKey = new IntWritable(currentQueryIter.nextIndex());
      List<T> values = search(indexSearcher, currentQueryIter.next());
      currentValueIterSize = values.size();
      currentValueIterPosition = 0f;
      currentValueIter = values.iterator();
      return nextKeyValue();
    }

    // no more currentValues nor queries in this index, move to the next index
    if (currentIndexPathIter.hasNext()) {
      // TODO: does the old one need to be closed?
      IndexReader indexReader = openIndex(currentIndexPathIter.next(), conf);
      indexSearcher = createSearcher(indexReader);
      currentQueryIter = queries.listIterator();
      return nextKeyValue();
    }

    // all done
    return false;
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
   * It splits the progress equally among all indexes, then among all queries for that index,
   * then among all return values from the {@link #search} method.
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

    float currentQueryProgress = currentValueIterSize == 0 ? 1f
                                 : currentValueIterPosition / currentValueIterSize;

    return indexProgress
           + queriesProgress
           + (currentQueryProgress * (1.0f / ((float) numIndexes * (float) queries.size())));
  }
}
