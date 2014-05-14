package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public interface WorkFileOverride {
  public static abstract class FileOutputFormat<K, V>
    extends org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<K, V> implements WorkFileOverride {
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
      return WorkFileOverride.Method.getDefaultWorkFileOverride(this, this, context, extension);
    }

    private String name;
    @Override public void setName(String name) { this.name = name; }
    @Override public String getName() { return name; }
  }

  public static abstract class TextOutputFormat<K, V>
      extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat<K, V> implements WorkFileOverride {
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
      return WorkFileOverride.Method.getDefaultWorkFileOverride(this, this, context, extension);
    }

    private String name;
    @Override public void setName(String name) { this.name = name; }
    @Override public String getName() { return name; }
  }

  public static class Method {
    private Method() {
    }

    public static Path getDefaultWorkFileOverride(
      WorkFileOverride workFileOverride,
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<?, ?> outputFormat,
      TaskAttemptContext context,
      String extension
    ) throws IOException {
      String name = workFileOverride.getName();
      if (name == null) {
        return outputFormat.getDefaultWorkFile(context, extension);
      } else {
        FileOutputCommitter committer = (FileOutputCommitter) outputFormat.getOutputCommitter(context);
        return new Path(committer.getWorkPath(), name + extension);
      }
    }
  }

  void setName(String name);
  String getName();
}
