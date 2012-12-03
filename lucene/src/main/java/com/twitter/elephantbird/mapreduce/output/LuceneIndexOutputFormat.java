package com.twitter.elephantbird.mapreduce.output;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import com.twitter.elephantbird.util.HdfsUtils;
import com.twitter.elephantbird.util.ReducerHeartbeatThread;

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
  private static final Logger LOG = Logger.getLogger(LuceneIndexOutputFormat.class.getName());

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
   * Override this method if you intend to use an {@link Analyzer} other than the default
   * {@link WhitespaceAnalyzer} during index creation.
   *
   * NOTE: Even if you don't plan on using an {@link Analyzer} at all (meaning you will build your
   * {@link Document}s using methods that don't invoke analysis, an {@link Analyzer} still has to
   * be supplied to {@link IndexWriter}
   * TODO: consider using an analyzer that always throws exceptions to ensure
   * TODO: it's not used by mistake
   *
   * @param conf the job's configuration
   * @return an {@link Analyzer} suitable for use by an {@link IndexWriter}
   */
  protected Analyzer newAnalyzer(Configuration conf) {
    return new WhitespaceAnalyzer(Version.LUCENE_40);
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException {
    FileOutputCommitter committer = (FileOutputCommitter) this.getOutputCommitter(job);

    File tmpDirFile = Files.createTempDir();
    // TODO: Can we use NIOFS? Is there good reason to? Kyle warned against it
    FSDirectory tmpDirLucene = new SimpleFSDirectory(tmpDirFile, NoLockFactory.getNoLockFactory());

    // TODO: Is there a non-analyzer constructor for this?
    IndexWriterConfig idxConfig = new IndexWriterConfig(Version.LUCENE_40,
      newAnalyzer(job.getConfiguration()));

    // TODO: Find out what these actually do and if they're the right choices
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    idxConfig.setMergePolicy(mergePolicy);
    idxConfig.setMergeScheduler(new SerialMergeScheduler());

    IndexWriter writer = new IndexWriter(tmpDirLucene, idxConfig);
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
      ReducerHeartbeatThread heartBeat = new ReducerHeartbeatThread(context) {
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
        Path out = new Path(work, "index-"
            + String.valueOf(context.getTaskAttemptID().getTaskID().getId()));

        writer.forceMerge(1);
        writer.close();

        FileSystem fs = out.getFileSystem(context.getConfiguration());
        File[] files = tmpDirFile.listFiles();
        if (files != null) {
          for (File file : files) {
            LOG.info("Copying " + file.getName());
            fs.copyFromLocalFile(
              true,
              true,
              new Path(file.getAbsolutePath()),
              new Path(out, file.getName()));
          }
        }

        LOG.info("Deleting tmpdir");
        FileUtils.deleteDirectory(tmpDirFile);
        LOG.info("Index written to: " + out);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error committing index", e);
        throw e;
      } finally {
        // all things must die, eventually
        LOG.info("Stopping heartbeat thread");
        heartBeat.stop();
      }
    }
  }

  /**
   * Set where the index should be written to
   *
   * @param job the job
   * @param path where to write the index
   */
  public static void setOutputPath(Job job, Path path) {
    FileOutputFormat.setOutputPath(job, path);
  }

  /**
   * Accepts non-hidden directories that start with "index-"
   * This is what the indexes created by this output format look like,
   * so this is useful for finding them when traversing the file system
   */
  public static class IndexDirFilter implements PathFilter {
    private Configuration conf;

    public IndexDirFilter(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public boolean accept(Path path) {
      if (!HdfsUtils.EXCLUDE_HIDDEN_PATHS_FILTER.accept(path)) {
        return false;
      }
      try {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus fileStatus = fs.getFileStatus(path);
        return fileStatus.isDir() && path.getName().startsWith("index-");
      } catch (IOException e) {
        throw new RuntimeException("Could not get file status for path " + path, e);
      }
    }
  }
}
