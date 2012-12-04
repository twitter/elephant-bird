package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.io.IntWritable;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

/**
 * Only counts the number of hits for each query
 *
 * @author Alex Levenson
 */
public abstract class LuceneIndexCountHitsRecordReader
    extends LuceneIndexRecordReader<IntWritable> {

  @Override
  protected List<IntWritable> search(IndexSearcher searcher, Query query) throws IOException {
    CountHitsCollector collector = new CountHitsCollector();
    searcher.search(query, collector);
    return ImmutableList.of(new IntWritable(collector.getHits()));
  }

  private static final class CountHitsCollector extends Collector {
    private int hits = 0;

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int i) throws IOException {
      hits++;
    }

    @Override
    public void setNextReader(AtomicReaderContext atomicReaderContext) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

    public int getHits() {
      return hits;
    }
  }
}
