package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public interface WorkFileOverride {
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

  void setName(String name);
}
