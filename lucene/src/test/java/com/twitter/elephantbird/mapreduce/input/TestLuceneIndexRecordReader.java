package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Test;

import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat.LuceneIndexInputSplit;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Levenson
 */
public class TestLuceneIndexRecordReader extends EasyMockSupport {

  @Test
  @SuppressWarnings("unchecked")
  public void testOneIndexMultipleQueries() throws Exception {
    ArrayList<ArrayList<ArrayList<Integer>>> indexesQueriesDocIds = Lists.newArrayList();
    indexesQueriesDocIds.add(Lists.<ArrayList<Integer>>newArrayList(
      Lists.newArrayList(0, 1, 2),
      Lists.<Integer>newArrayList(),
      Lists.newArrayList(3, 4)
    ));

    testLuceneIndexRecordReader(
        Lists.newArrayList("query1", "query2", "query3"),
        Lists.newArrayList(new Path("/mock/index")),
        indexesQueriesDocIds);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultipleIndexesOneQuery() throws Exception {
    ArrayList<ArrayList<ArrayList<Integer>>> indexesQueriesDocIds = Lists.newArrayList();

    ArrayList<ArrayList<Integer>> index1 = Lists.newArrayList();
    index1.add(Lists.newArrayList(0, 1, 2));

    ArrayList<ArrayList<Integer>> index2 = Lists.newArrayList();
    index2.add(Lists.<Integer>newArrayList());
    ArrayList<ArrayList<Integer>> index3 = Lists.newArrayList();
    index3.add(Lists.newArrayList(3, 4));

    indexesQueriesDocIds.add(index1);
    indexesQueriesDocIds.add(index2);
    indexesQueriesDocIds.add(index3);

    testLuceneIndexRecordReader(Lists.newArrayList("query1"),
      Lists.newArrayList(new Path("/mock/index1"),
        new Path("/mock/index2"),
        new Path("/mock/index3")),
      indexesQueriesDocIds);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultipleIndexesMultipleQueries() throws Exception {
    ArrayList<ArrayList<ArrayList<Integer>>> indexesQueriesDocIds = Lists.newArrayList();

    ArrayList<ArrayList<Integer>> index1 = Lists.newArrayList();
    index1.add(Lists.newArrayList(0, 1));
    index1.add(Lists.newArrayList(2));

    ArrayList<ArrayList<Integer>> index2 = Lists.newArrayList();
    index2.add(Lists.<Integer>newArrayList());
    index2.add(Lists.<Integer>newArrayList());

    ArrayList<ArrayList<Integer>> index3 = Lists.newArrayList();
    index3.add(Lists.newArrayList(3));
    index3.add(Lists.newArrayList(4));

    indexesQueriesDocIds.add(index1);
    indexesQueriesDocIds.add(index2);
    indexesQueriesDocIds.add(index3);

    testLuceneIndexRecordReader(
      Lists.newArrayList("query1", "query2"),
      Lists.newArrayList(
        new Path("/mock/index1"),
        new Path("/mock/index2"),
        new Path("/mock/index3")),
      indexesQueriesDocIds);
  }

  // Document is final and therefore not easy to mock
  // so this is the ugly work around
  private static final Document[] docs = new Document[]{
    new Document(),
    new Document(),
    new Document(),
    new Document(),
    new Document()
  };

  private static final Map<Document, Integer> docsAndValues = ImmutableMap.of(
    docs[0], 10,
    docs[1], 11,
    docs[2], 12,
    docs[3], 13,
    docs[4], 14
  );

  private static abstract class MockRecordReader extends LuceneIndexCollectAllRecordReader<IntWritable> {
    @Override
    protected IntWritable docToValue(Document doc) {
      return new IntWritable(Integer.valueOf(docsAndValues.get(doc)));
    }

    @Override
    protected void closeIndexReader(IndexReader reader) throws IOException {

    }
  }

  private void testLuceneIndexRecordReader(ArrayList<String> queryStrings,
      ArrayList<Path> indexPaths,
      ArrayList<ArrayList<ArrayList<Integer>>> indexesQueriesDocIds)
      throws Exception {

    LuceneIndexInputSplit split = createStrictMock(LuceneIndexInputSplit.class);
    expect(split.getIndexDirs()).andReturn(indexPaths);
    replay(split);

    Configuration conf = new Configuration();
    TaskAttemptContext context = createStrictMock(TaskAttemptContext.class);
    expect(HadoopCompat.getConfiguration(context)).andStubReturn(conf);
    ((Progressable)context).progress(); // casting to avoid Hadoop 2 incompatibility
    expectLastCall().atLeastOnce();
    replay(context);

    LuceneIndexInputFormat.setQueries(queryStrings, conf);

    LuceneIndexRecordReader<IntWritable> rr =
        createMockBuilder(MockRecordReader.class)
        .addMockedMethod("openIndex")
        .addMockedMethod("createSearcher")
        .createMock();

    Query[] queries = new Query[queryStrings.size()];
    for (int i = 0; i < queries.length; i++) {
      Query query = createStrictMock(Query.class);
      replay(query);
      queries[i] = query;
      expect(rr.deserializeQuery(queryStrings.get(i))).andReturn(query);
    }

    for (int index = 0; index < indexPaths.size(); index++) {
      IndexReader reader = createStrictMock(IndexReader.class);
      expect(reader.maxDoc()).andStubReturn(4);
      replay(reader);
      expect(rr.openIndex(indexPaths.get(index), conf)).andReturn(reader);

      IndexSearcher searcher = createStrictMock(IndexSearcher.class);
      expect(rr.createSearcher(reader)).andReturn(searcher);

      for (int query = 0; query < queries.length; query++) {
        final ArrayList<Integer> ids = indexesQueriesDocIds.get(index).get(query);
        final Capture<Collector> collectorCapture = new Capture<Collector>();
        expect(searcher.getIndexReader()).andReturn(reader);
        searcher.search(eq(queries[query]), capture(collectorCapture));

        expectLastCall().andAnswer(new IAnswer<Void>() {
          @Override
          public Void answer() throws Throwable {
            for (int id : ids) {
              collectorCapture.getValue().collect(id);
            }
            return null;
          }
        });



        for (int docId : ids) {
          expect(searcher.doc(docId)).andReturn(docs[docId]);
        }
      }
      replay(searcher);
    }

    replay(rr);

    rr.initialize(split, context);

    float prevProgress = -1;
    for (int index = 0; index < indexesQueriesDocIds.size(); index++) {
      for (int query = 0; query < indexesQueriesDocIds.get(index).size(); query++) {
        for (int docId : indexesQueriesDocIds.get(index).get(query)) {
          assertTrue(rr.nextKeyValue());
          assertEquals(query, rr.getCurrentKey().get());
          assertEquals(docsAndValues.get(docs[docId]), (Integer) rr.getCurrentValue().get());
          float newProgress = rr.getProgress();
          assertTrue(newProgress > prevProgress);
          assertTrue(newProgress <= 1.0);
        }
      }
    }

    assertFalse(rr.nextKeyValue());
    assertFalse(rr.nextKeyValue());

    verifyAll();
  }
}
