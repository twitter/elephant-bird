package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This is a class to deal with the different handling of subpaths in the mapred
 * and mapreduce api. The WorkFileOverride interface is used by the
 * {@link DeprecatedFileOutputFormatWrapper} in order to know whether a name
 * has been passed to
 * {@link org.apache.hadoop.mapred.OutputFormat#getRecordWriter(FileSystem, JobConf, String, Progressable}.
 * Historically, these were thrown away which was ok except in the case where
 * a map reduce job wanted to write to multiple sub-directories in the base
 * path. Using the WorkFileOverride will make it so that this is possible.
 * This is achieved by overriding
 * {@link org.apache.hadoop.mapreduce.OutputFormat#getDefaultWorkFile(TaskAttemptContext, String}} and
 * incoroprating a name variable. the {@link #setName()} method serves as a hook so that the
 * {@link DeprecatedFileOutputFormatWrapper} can pass the name from the mapred api to the mapreduce API.
 *
 * @author Jonathan Coveney
 */
public interface WorkFileOverride {
  /**
   * This is an abstract base class that users can drop in as a replacement for
   * using the normal FileOutputFormat as a base class in order to make the
   * extending subclass usable with {@link DeprecatedFileOutputFormatWrapper}.
   *
   * {@see com.twitter.elephantbird.mapreduce.output.LzoOutputFormat}
   */
  public static abstract class FileOutputFormat<K, V>
      extends org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<K, V> implements WorkFileOverride {

    private String name;

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
      if (name == null) {
        return super.getDefaultWorkFile(context, extension);
      } else {
        return WorkFileOverride.Method.getDefaultWorkFileOverride(this, name, context, extension);
      }
    }

    @Override public void setName(String name) { this.name = name; }
  }

  /**
   * This is an abstract base class that users can drop in as a replacement for
   * using the normal TextOutputFormat as a base class in order to make the
   * extending subclass usable with {@link DeprecatedFileOutputFormatWrapper}.
   *
   * {@see com.twitter.elephantbird.mapreduce.output.LzoTextOutputFormat}
   */
  public static abstract class TextOutputFormat<K, V>
      extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat<K, V> implements WorkFileOverride {

    private String name;

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
      if (name == null) {
        return super.getDefaultWorkFile(context, extension);
      } else {
        return WorkFileOverride.Method.getDefaultWorkFileOverride(this, name, context, extension);
      }
    }

    @Override public void setName(String name) { this.name = name; }
  }

  /**
   * This class is a helper to minimize the amount of logic to implement for
   * a given FileOutputFormat that wants to implement WorkFileOverride.
   */
  public static class Method {
    private Method() {
    }

    public static Path getDefaultWorkFileOverride(
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<?, ?> outputFormat,
      String name,
      TaskAttemptContext context,
      String extension
    ) throws IOException {
      FileOutputCommitter committer = (FileOutputCommitter) outputFormat.getOutputCommitter(context);
      return new Path(committer.getWorkPath(), name + extension);
    }
  }

  /**
   * This is the hook for a OutputFormat to pass in a subpath.
   */
  void setName(String name);
}
