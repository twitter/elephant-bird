package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.io.IntWritable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;

/**
 * Only counts the number of hits for each query
 *
 * @author Alex Levenson
 */
public abstract class LuceneIndexCountHitsRecordReader
    extends LuceneIndexRecordReader<IntWritable> {

  @Override
  protected Iterator<IntWritable> search(IndexSearcher searcher, Query query) throws IOException {
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    return ImmutableList.of(new IntWritable(collector.getTotalHits())).iterator();
  }
}
