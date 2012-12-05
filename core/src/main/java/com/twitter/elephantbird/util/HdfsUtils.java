package com.twitter.elephantbird.util;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
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
   * @param filter filter to determine which paths to accept
   * @param conf hadoop conf
   * @param visitor visitor to apply to each accepted path
   * @throws IOException
   */
  public static void walkPath(Path path,
                              PathFilter filter,
                              Configuration conf,
                              PathVisitor visitor) throws IOException {

    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);

    if (filter.accept(path)) {
      visitor.visit(fileStatus);
    }

    if (fileStatus.isDir()) {
      FileStatus[] children = fs.listStatus(path);
      for (FileStatus childStatus : children) {
        walkPath(childStatus.getPath(), filter, conf, visitor);
      }
    }
  }

  /**
   * Recursively walk a path, adding paths that are accepted by filter to accumulator
   *
   * @param path root path to begin walking, will be added to accumulator
   * @param filter filter to determine which paths to accept
   * @param conf hadoop conf
   * @param accumulator all paths accepted will be added to accumulator
   * @throws IOException
   */
  public static void collectPaths(Path path,
                                  PathFilter filter,
                                  Configuration conf,
                                  final List<Path> accumulator) throws IOException {

    walkPath(path, filter, conf, new PathVisitor() {
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
   * Calculates the total size of a directory
   * and all of its contents (recursively)
   *
   * @param path path to recursively walk
   * @param conf job config
   * @return size of the directory in bytes
   * @throws IOException
   */
  public static long getDirectorySize(Path path, Configuration conf) throws IOException {
    PathSizeVisitor visitor = new PathSizeVisitor();
    walkPath(path, PathFilters.ACCEPT_ALL_PATHS_FILTER, conf, visitor);
    return visitor.getSize();
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
      for (FileStatus status : statuses) {
        paths.add(status.getPath());
      }
    }

    return paths;
  }
}
