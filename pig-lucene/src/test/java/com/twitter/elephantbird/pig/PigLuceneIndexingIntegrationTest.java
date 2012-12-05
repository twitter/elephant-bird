package com.twitter.elephantbird.pig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.pig.ExecType;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.twitter.elephantbird.mapreduce.LuceneIndexingIntegrationTest;
import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat;
import com.twitter.elephantbird.pig.load.LuceneIndexLoader;
import com.twitter.elephantbird.pig.store.LuceneIndexStorage;

import static org.junit.Assert.assertEquals;

/**
 * End-to-end test of
 * {@link com.twitter.elephantbird.pig.store.LuceneIndexStorage} and {@link LuceneIndexLoader}
 * <p>
 * Similar to {@link LuceneIndexingIntegrationTest}
 * <ol>
 *   <li>Builds three indexes of small documents (text from the Iliad and Macbeth)</li>
 *   <li>Searches the indexes using queries supplied as string literals</li>
 *   <li>Verifies that the correct results are found</li>
 *   <li>Searches the indexes using queries stored in a file</li>
 *   <li>Verifies that the correct results are found</li>
 * </ol>
 *
 * @author Alex Levenson
 */
public class PigLuceneIndexingIntegrationTest {

  public static class IndexOutputFormat extends LuceneIndexStorage.PigLuceneIndexOutputFormat {
    private LuceneIndexingIntegrationTest.IndexOutputFormat delegate =
      new LuceneIndexingIntegrationTest.IndexOutputFormat();

    private static final Text text = new Text();

    @Override
    protected Document buildDocument(NullWritable key, Tuple value) throws IOException {
      text.set((String) value.get(0));
      return delegate.buildDocument(key, text);
    }

    @Override
    protected Analyzer newAnalyzer(Configuration conf) {
      return delegate.newAnalyzer(conf);
    }
  }

  public static class Loader extends LuceneIndexLoader<Text> implements LoadMetadata {
    private static final TupleFactory TF = TupleFactory.getInstance();
    public Loader(String[] args) {
      super(args);
    }

    @Override
    protected Tuple recordToTuple(int key, Text value) {
      return TF.newTuple(ImmutableList.of(key, value.toString()));
    }

    @Override
    protected LuceneIndexInputFormat<Text> getLuceneIndexInputFormat() throws IOException {
      return new LuceneIndexingIntegrationTest.IndexInputFormat();
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
      return new ResourceSchema(Utils.getSchemaFromString("queryId:int, text:chararray"));
    }

    @Override
    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
      return null;
    }

    @Override
    public String[] getPartitionKeys(String s, Job job) throws IOException {
      return new String[0];
    }

    @Override
    public void setPartitionFilter(Expression expression) throws IOException {
    }
  }

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testIndexing() throws Exception {
    for (Path input : LuceneIndexingIntegrationTest.INPUT_PATHS) {
      // the input files are in the lucene module's resources but the
      // cwd is the pig-lucene module's directory
      input = new Path(new File("../lucene/" + input.toString()).getAbsolutePath());

      PigServer pigServer = new PigServer(ExecType.LOCAL);
      pigServer.setBatchOn();
      Map<String, String> params = ImmutableMap.of(
        "INPUT", input.toString(),
        "OUTPUT", new File(tempDir.getRoot(), input.getName()).getAbsolutePath()
      );
      pigServer.registerScript(
        "src/test/resources/com/twitter/elephantbird/pig/index.pig",
        params);

      runPigScript(pigServer, "Indexing of " + input + " failed!");
    }

    File resultsQueries = new File(tempDir.getRoot(), "results_queries");
    File resultsFile = new File(tempDir.getRoot(), "results_file");
    File queryFile = tempDir.newFile("queryfile.txt");

    Map<String, String> paramsQueries = ImmutableMap.of(
      "INPUT", tempDir.getRoot().getAbsolutePath(),
      "OUTPUT", resultsQueries.getAbsolutePath()
    );

    Map<String, String> paramsFile = ImmutableMap.of(
      "INPUT", tempDir.getRoot().getAbsolutePath(),
      "OUTPUT", resultsFile.getAbsolutePath(),
      "QUERY_FILE", queryFile.getAbsolutePath()
    );

    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setBatchOn();

    pigServer.registerScript(
      "src/test/resources/com/twitter/elephantbird/pig/search_queries.pig",
       paramsQueries);

    runPigScript(pigServer, "Searching via string literal queries failed!");

    assertEquals(LuceneIndexingIntegrationTest.expectedResults,
      LuceneIndexingIntegrationTest.parseResultsFile(new File(resultsQueries, "part-m-00000")));

    // write the queries to a file to test loading queries from a file
    Writer out = new OutputStreamWriter(new FileOutputStream(queryFile));
    for (String query : LuceneIndexingIntegrationTest.QUERIES) {
      out.write(query);
      out.write("\n");
    }
    out.close();

    pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setBatchOn();

    pigServer.registerScript(
      "src/test/resources/com/twitter/elephantbird/pig/search_file.pig",
      paramsFile);

    runPigScript(pigServer, "Searching via queries loaded from a file failed!");

    assertEquals(LuceneIndexingIntegrationTest.expectedResults,
      LuceneIndexingIntegrationTest.parseResultsFile(new File(resultsFile, "part-m-00000")));

  }

  private void runPigScript(PigServer server, String failureMessage) throws IOException {
    List<ExecJob> jobs = server.executeBatch();
    for (ExecJob job : jobs) {
      if (job.getStatus() != ExecJob.JOB_STATUS.COMPLETED) {
        throw new RuntimeException(failureMessage);
      }
    }
  }
}