package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
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

  // TODO: This bit set can get big (up to about Integer.MAX_VALUE bits),
  // TODO: ideally it should get thrown away when its no longer in use.
  // TODO: However, it's difficult to know when that is, and the jvm will be torn down after
  // TODO: each map task anyway.
  private static final BitSet docIds = new BitSet();

  /**
   * Convert a {@link Document} to a value to be emitted by this record reader
   *
   * @param doc document to convert
   * @return a value to be emitted from this record reader
   */
  protected abstract T docToValue(Document doc) throws IOException;

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
  protected Iterator<T> search(final IndexSearcher searcher, final Query query) throws IOException {
    // grow the bit set if needed
    docIds.set(searcher.getIndexReader().maxDoc());
    // clear it
    docIds.clear();
    searcher.search(query, new Collector() {
      private int docBase;

      @Override
      public void setScorer(Scorer scorer) {
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }

      @Override
      public void collect(int doc) {
        docIds.set(doc + docBase);
      }

      public void setNextReader(AtomicReaderContext context) {
        this.docBase = context.docBase;
      }
    });

    return new AbstractIterator<T>() {
      private int doc = docIds.nextSetBit(0);

      @Override
      protected T computeNext() {
        ((Progressable)context).progress(); // casting to avoid Hadoop 2 incompatibility
        if (doc < 0) {
          return endOfData();
        }
        try {
          T ret = docToValue(searcher.doc(doc));
          doc = docIds.nextSetBit(doc + 1);
          return  ret;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
