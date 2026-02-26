package com.twitter.elephantbird.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.twitter.elephantbird.lucene.HdfsMergeTool;
import com.twitter.elephantbird.mapreduce.input.LuceneIndexCollectAllRecordReader;
import com.twitter.elephantbird.mapreduce.input.LuceneIndexInputFormat;
import com.twitter.elephantbird.mapreduce.output.LuceneIndexOutputFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test of {@link LuceneIndexOutputFormat} and {@link LuceneIndexInputFormat}
 * <ol>
 *   <li>Builds three indexes of small documents (text from the Iliad and Macbeth)</li>
 *   <li>Searches the indexes</li>
 *   <li>Verifies that the correct results are found</li>
 * </ol>
 *
 * @author Alex Levenson
 */
public class LuceneIndexingIntegrationTest {

  public static final Path[] INPUT_PATHS = new Path[]{
    new Path("src/test/resources/com/twitter/elephantbird/mapreduce/test_documents1.txt"),
    new Path("src/test/resources/com/twitter/elephantbird/mapreduce/test_documents2.txt"),
    new Path("src/test/resources/com/twitter/elephantbird/mapreduce/test_documents3.txt")
  };

  public static final List<String> QUERIES =
    Lists.newArrayList("+(macbeth achilles)", "+shield", "+dusty +death.");

  public static final Map<Integer, Set<String>> expectedResults;

  static {
    expectedResults = Maps.newHashMap();
    expectedResults.put(1, Sets.newHashSet(
      "Then when he had fashioned the shield so great and strong, "
        + "he made a breastplate also that shone brighter than fire.",
      "He made the shield in five thicknesses, and with many a wonder did "
        + "his cunning hand enrich it.",
      "All round the outermost rim of the shield he set the mighty stream of the river Oceanus.",
      "First he shaped the shield so great and strong, adorning it all over and binding "
        + "it round with a gleaming circuit in three layers;"));
    expectedResults.put(2, Sets.newHashSet("The way to dusty death. Out, out, brief candle! "
      + "Life's but a walking shadow, a poor player"));
  }

  public static class IndexOutputFormat extends LuceneIndexOutputFormat<NullWritable, Text> {
    private final Document doc;
    private final Field textField;

    public IndexOutputFormat() {
      doc = new Document();
      textField = new TextField("text", "", Field.Store.YES);
      doc.add(textField);
    }

    @Override
    public Document buildDocument(NullWritable key, Text value) throws IOException {
      textField.setStringValue(value.toString());
      return doc;
    }

    @Override
    public Analyzer newAnalyzer(Configuration conf) {
      return new WhitespaceAnalyzer(Version.LUCENE_40);
    }
  }

  private static class IndexMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      context.write(NullWritable.get(), value);
    }
  }

  private static class IndexReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      for (Text value : values) {
        context.write(NullWritable.get(), value);
      }
    }
  }

  public static class IndexInputFormat extends LuceneIndexInputFormat<Text> {

    @Override
    public PathFilter getIndexDirPathFilter(Configuration conf) throws IOException {
      return LuceneIndexOutputFormat.newIndexDirFilter(conf);
    }

    @Override
    public RecordReader<IntWritable, Text>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

      return new LuceneIndexCollectAllRecordReader<Text>() {
        private QueryParser parser = new QueryParser(Version.LUCENE_40,
            "text",
             new WhitespaceAnalyzer(Version.LUCENE_40));

        private Text text = new Text();

        @Override
        protected Query deserializeQuery(String serializedQuery) throws IOException {
          try {
            return parser.parse(serializedQuery);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        protected Text docToValue(Document doc) {
          text.set(doc.get("text"));
          return text;
        }
      };
    }
  }

  private static class SearchMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      context.write(key, value);
    }
  }

  private static class SearchReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      for (Text value : values) {
        context.write(key, value);
      }
    }
  }

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testIndexing() throws Exception {
    List<Path> indexes = Lists.newLinkedList();
    for (Path input : INPUT_PATHS) {
      Job job = new Job();

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(IndexOutputFormat.class);

      job.setMapperClass(IndexMapper.class);
      job.setReducerClass(IndexReducer.class);

      job.setOutputValueClass(Text.class);
      job.setOutputKeyClass(NullWritable.class);

      TextInputFormat.setInputPaths(job, input);

      Path output = new Path(
          new File(tempDir.getRoot(), input.getName()).getAbsolutePath());

      indexes.add(output);

      IndexOutputFormat.setOutputPath(job, output);

      assertTrue("Indexing of " + input + " failed!", job.waitForCompletion(true));
    }

    doSearch(indexes,
             new Path(new File(tempDir.getRoot(), "search_results").getAbsolutePath()),
             "Failed searching un-merged indexes");

    File mergeIndex = new File(tempDir.getRoot(), "index-merged");
    String[] args = new String[indexes.size() + 2];
    args[0] = mergeIndex.getAbsolutePath();
    args[1] = "100";
    int i = 2;
    for (Path indexPath : indexes) {
      args[i++] = indexPath.toString() + "/index-0";
    }
    HdfsMergeTool.main(args);

    doSearch(Lists.newArrayList(new Path(mergeIndex.getAbsolutePath())),
             new Path(new File(tempDir.getRoot(), "merge_search_results").getAbsolutePath()),
             "Failed searching merged index");
  }

  private void doSearch(List<Path> inputPaths, Path outputPath, String failureMessage) throws Exception {
    Job job = new Job();

    job.setInputFormatClass(IndexInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(SearchMapper.class);
    job.setReducerClass(SearchReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    IndexInputFormat.setInputPaths(inputPaths, HadoopCompat.getConfiguration(job));

    IndexInputFormat.setQueries(QUERIES, job.getConfiguration());

    TextOutputFormat.setOutputPath(job, outputPath);

    assertTrue(failureMessage, job.waitForCompletion(true));

    File resultsFile = new File(new File(outputPath.toString()), "part-r-00000");

    assertEquals(expectedResults, parseResultsFile(resultsFile));
  }

  public static Map<Integer, Set<String>> parseResultsFile(File file) throws IOException {
    return Files.readLines(file, Charsets.UTF_8,
      new LineProcessor<Map<Integer, Set<String>>>() {
        private Map<Integer, Set<String>> results = Maps.newHashMap();

        @Override
        public boolean processLine(String s) throws IOException {
          String[] parts = s.split("\t");
          int query = Integer.valueOf(parts[0]);
          if (results.get(query) == null) {
            results.put(query, Sets.<String>newHashSet());
          }
          results.get(query).add(parts[1]);
          return true;
        }

        @Override
        public Map<Integer, Set<String>> getResult() {
          return results;
        }
      });
  }
}