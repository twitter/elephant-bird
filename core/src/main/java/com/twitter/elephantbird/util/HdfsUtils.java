package com.twitter.elephantbird.util;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * HDFS utilities
 */
public final class HdfsUtils {
  private HdfsUtils() { }

  /**
   * Converts a path to a qualified string
   */
  public static class PathToQualifiedString implements Function<Path, String> {
    private Configuration conf;

    public PathToQualifiedString(Configuration conf) {
      this.conf = Preconditions.checkNotNull(conf);
    }

    @Override
    public String apply(Path path) {
      try {
        return path.getFileSystem(conf).makeQualified(path).toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Used by {@link HdfsUtils#walkPath} to 'visit' or process a
   * path
   */
  public static interface PathVisitor {
    void visit(FileStatus fileStatus);
  }

  /**
   * Recursively walk a path applying visitor to each path accepted by
   * filter
   *
   * @param path root path to begin walking, will be visited if
   *             it passes the filter and directory flag
   * @param fs FileSystem for this path
   * @param filter filter to determine which paths to accept
   * @param visitor visitor to apply to each accepted path
   * @throws IOException
   */
  public static void walkPath(Path path,
                              FileSystem fs,
                              PathFilter filter,
                              PathVisitor visitor) throws IOException {

    FileStatus fileStatus = fs.getFileStatus(path);

    if (filter.accept(path)) {
      visitor.visit(fileStatus);
    }

    if (fileStatus.isDir()) {
      FileStatus[] children = fs.listStatus(path);
      for (FileStatus childStatus : children) {
        walkPath(childStatus.getPath(), fs, filter, visitor);
      }
    }
  }

  /**
   * Recursively walk a path, adding paths that are accepted by filter to accumulator
   *
   * @param path root path to begin walking, will be added to accumulator
   * @param fs FileSystem for this path
   * @param filter filter to determine which paths to accept
   * @param accumulator all paths accepted will be added to accumulator
   * @throws IOException
   */
  public static void collectPaths(Path path,
                                  FileSystem fs,
                                  PathFilter filter,
                                  final List<Path> accumulator) throws IOException {

    walkPath(path, fs, filter, new PathVisitor() {
      @Override
      public void visit(FileStatus fileStatus) {
        accumulator.add(fileStatus.getPath());
      }
    });
  }

  private static class PathSizeVisitor implements PathVisitor {
    private long size = 0;

    @Override
    public void visit(FileStatus fileStatus) {
      size += fileStatus.getLen();
    }

    public long getSize() {
      return size;
    }
  }

  /**
   * Calculates the total size of all the contents of a directory that are accepted
   * by filter. All subdirectories will be searched recursively and paths in subdirectories
   * that are accepted by filter will also be counted.
   *
   * Does not include the size of directories themselves
   * (which are 0 in HDFS but may not be 0 on local file systems)
   *
   * To get the size of a directory without filtering, use
   * {@link #getDirectorySize(Path, FileSystem)} which is much more efficient.
   *
   * @param path path to recursively walk
   * @param fs FileSystem for this path
   * @param filter path filter for which paths size's to include in the total
   *               NOTE: you do *not* need to filter out directories, this will be done for you
   * @return size of the directory in bytes
   * @throws IOException
   */
  public static long getDirectorySize(Path path, FileSystem fs, PathFilter filter)
      throws IOException {
    PathSizeVisitor visitor = new PathSizeVisitor();
    PathFilter composite = new PathFilters.CompositePathFilter(
      PathFilters.newExcludeDirectoriesFilter(fs.getConf()),
      filter);
    walkPath(path, fs, composite, visitor);
    return visitor.getSize();
  }

  /**
   * Calculates the total size of all the contents of a directory,
   * including the contents of all of its subdirectories.
   * Does not include the size of directories themselves
   * (which are 0 in HDFS but may not be 0 on local file systems)
   *
   * @param path path to recursively walk
   * @param fs FileSystem for this path
   * @return size of the directory's contents in bytes
   * @throws IOException
   */
  public static long getDirectorySize(Path path, FileSystem fs) throws IOException {
    ContentSummary cs = fs.getContentSummary(path);
    return cs.getLength();
  }

  /**
   * Given a list of paths that (potentially) have glob syntax in them,
   * return a list of paths with all the globs expanded.
   *
   * @param pathsWithGlobs a list of paths that may or may not have glob syntax in them
   * @param conf job conf
   * @return an equivalent list of paths with no glob syntax in them
   * @throws IOException
   */
  public static List<Path> expandGlobs(List<String> pathsWithGlobs, Configuration conf)
    throws IOException {

    List<Path> paths = Lists.newLinkedList();
    for (String pathStr : pathsWithGlobs) {
      Path path = new Path(pathStr);
      FileSystem fs = path.getFileSystem(conf);
      FileStatus[] statuses = fs.globStatus(path);
      // some versions of hadoop return null for non-existent paths
      if (statuses != null) {
        for (FileStatus status : statuses) {
          paths.add(status.getPath());
        }
      }
    }
    return paths;
  }
}
