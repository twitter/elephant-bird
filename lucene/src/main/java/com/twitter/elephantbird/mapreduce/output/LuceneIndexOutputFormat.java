package com.twitter.elephantbird.mapreduce.output;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

import com.google.common.io.Files;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.util.PathFilters;
import com.twitter.elephantbird.util.TaskHeartbeatThread;

/**
 * Base class for output formats that write lucene indexes
 * <p>
 * Subclasses must specify how to convert a key value pair into a {@link Document}
 * <p>
 * Subclasses may provide an {@link Analyzer} to use during index creation
 * (which may be used depending on how documents are created by the subclass)
 *
 * @author Alex Levenson, based on code written by Kyle Maxwell
 */
public abstract class LuceneIndexOutputFormat<K, V> extends FileOutputFormat<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexOutputFormat.class);

  /**
   * Convert a record from the MR framework into a lucene {@link Document}
   * You may re-use the same {@link Document} instance for efficiency
   *
   * @param key the key written to this output format
   * @param value the value written to this output format
   * @return a lucene Document suitable for insertion into an {@link IndexWriter}
   */
  protected abstract Document buildDocument(K key, V value) throws IOException;

  /**
   * Override this method if you intend to use an {@link Analyzer}
   * during index creation. If you do not override this method, {@link NeverTokenizeAnalyzer} will
   * be used which will throw an exception if you create a {@link Document} using a method that
   * invokes tokenization.
   *
   * @param conf the job's configuration
   * @return an {@link Analyzer} suitable for use by an {@link IndexWriter}
   */
  protected Analyzer newAnalyzer(Configuration conf) {
    return new NeverTokenizeAnalyzer();
  }

  /**
   * Override to use a different {@link Directory} implementation
   *
   * You may want to use {@link org.apache.lucene.store.FSDirectory#open}
   * which is supposed to select an appropriate
   * local FS implementation based on the current OS. However, we have seen cases
   * where using this leads to an implementation that hits {@link java.lang.OutOfMemoryError}
   * when building large indexes.
   */
  protected Directory getDirectoryImplementation(File location) throws IOException {
    return new SimpleFSDirectory(location, NoLockFactory.getNoLockFactory());
  }

  public static IndexWriter createIndexWriter(Directory location, Analyzer analyzer) throws IOException {
    return createIndexWriter(location, analyzer, LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
  }

  public static IndexWriter createIndexWriter(Directory location, Analyzer analyzer, int mergeFactor)
      throws IOException {

    LOG.info("Creating IndexWriter with:\nDirectory: "
        + location
        + "\nAnalyzer: "
        + analyzer
        + "\nMerge Factor: " + mergeFactor);
    IndexWriterConfig idxConfig = new IndexWriterConfig(Version.LUCENE_40, analyzer);
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setMergeFactor(mergeFactor);
    mergePolicy.setUseCompoundFile(false);

    idxConfig.setMergePolicy(mergePolicy);

    idxConfig.setMergeScheduler(new SerialMergeScheduler());

    IndexWriter writer = new IndexWriter(location, idxConfig);
    return writer;
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException {
    FileOutputCommitter committer = (FileOutputCommitter) this.getOutputCommitter(job);
    File tmpDirFile = Files.createTempDir();
    Directory directory = getDirectoryImplementation(tmpDirFile);
    IndexWriter writer = createIndexWriter(directory, newAnalyzer(HadoopCompat.getConfiguration(job)));
    return new IndexRecordWriter(writer, committer, tmpDirFile);
  }

  private class IndexRecordWriter extends RecordWriter<K, V> {
    private IndexWriter writer;
    private FileOutputCommitter committer;
    private File tmpDirFile;
    private long recordsProcessed = 0;

    private IndexRecordWriter(IndexWriter writer, FileOutputCommitter committer, File tmpDirFile) {
      this.writer = writer;
      this.committer = committer;
      this.tmpDirFile = tmpDirFile;
    }

    @Override
    public void write(K key, V value) throws IOException {
      recordsProcessed++;
      if (recordsProcessed % 1000000 == 0) {
        LOG.info("Processing record " + recordsProcessed);
      }

      writer.addDocument(buildDocument(key, value));
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
      TaskHeartbeatThread heartBeat = new TaskHeartbeatThread(context) {
        @Override
        public void progress() {
          String[] filesLeft = tmpDirFile.list();
          if (filesLeft != null) {
            int remaining = filesLeft.length - 2;
            LOG.info("Optimizing " + remaining + " segments");
          } else {
            LOG.info("Done optimizing segments, heartbeat thread still alive");
          }
        }
      };

      try {
        LOG.info("Starting heartbeat thread");
        heartBeat.start();

        Path work = committer.getWorkPath();
        Path output = new Path(work, "index-"
            + String.valueOf(HadoopCompat.getTaskAttemptID(context).getTaskID().getId()));

        writer.forceMerge(1);
        writer.close();

        FileSystem fs = FileSystem.get(HadoopCompat.getConfiguration(context));
        LOG.info("Copying index to HDFS...");

        if (!FileUtil.copy(tmpDirFile, fs, output, true, HadoopCompat.getConfiguration(context))) {
          throw new IOException("Failed to copy local index to HDFS!");
        }

        LOG.info("Index written to: " + output);
      } catch (IOException e) {
        LOG.error("Error committing index", e);
        throw e;
      } finally {
        // all things must die, eventually
        LOG.info("Stopping heartbeat thread");
        heartBeat.stop();
      }
    }
  }

  /**
   * An analyzer that always throws {@link UnsupportedOperationException}
   * when {@link #createComponents(String, java.io.Reader)} is called
   *<p>
   * Useful if you don't intend to use an {@link Analyzer} for tokenization
   * but are required to provide one to an {@link org.apache.lucene.index.IndexWriter}
   */
  public static class NeverTokenizeAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Creates a path filter that accepts non-hidden directories that start with "index-"
   * This is what the indexes created by this output format look like,
   * so this is useful for finding them when traversing the file system
   */
  public static PathFilter newIndexDirFilter(Configuration conf) {
    return new PathFilters.CompositePathFilter(
      PathFilters.newExcludeFilesFilter(conf),
      PathFilters.EXCLUDE_HIDDEN_PATHS_FILTER,
      new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().startsWith("index-");
        }
      }
    );
  }
}
