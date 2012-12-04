package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

/**
 * Collects all documents that match queries
 *
 * @author Alex Levenson
 */
public abstract class LuceneIndexCollectAllRecordReader<T extends Writable>
    extends LuceneIndexRecordReader<T> {

  /**
   * Convert a {@link Document} to a value to be emitted by this record reader
   *
   * @param doc document to convert
   * @return a value to be emitted from this record reader
   */
  protected abstract T docToValue(Document doc);

  /**
   * Applies {@link #docToValue(Document)} to every document
   * found by executing query over searcher
   *
   * @param searcher the index searcher to query
   * @param query the query to run
   * @return a list of values to be emitted as records (one by one) by this record reader
   * @throws IOException
   */
  @Override
  protected List<T> search(final IndexSearcher searcher, Query query) throws IOException {
    CollectAll collector = new CollectAll();
    searcher.search(query, collector);
    return Lists.transform(collector.getHits(), new Function<Integer, T>() {
      @Override
      public T apply(Integer hit) {
        try {
          return docToValue(searcher.doc(hit));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private static final class CollectAll extends Collector {
    private int docBase;
    private ArrayList<Integer> hits = Lists.newArrayList();

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int i) throws IOException {
      hits.add(docBase + i);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      docBase = context.docBase;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

    public ArrayList<Integer> getHits() {
      return hits;
    }
  }
}
