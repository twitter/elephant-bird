package com.twitter.elephantbird.util;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Alex Levenson
 */
public class TestHdfsUtils {
  private static final String SAMPLE_DIR_LOCATION =
    "src/test/resources/com/twitter/elephantbird/util/";

  private static final Pattern SAMPLE_DIR_PATTERN =
      Pattern.compile(".*/" + SAMPLE_DIR_LOCATION + "(.*)");

  private static final Function<Path, String> PATH_TO_RELATIVE_STRING =
    new Function<Path, String>() {
      @Override
      public String apply(Path path) {
        Matcher m = SAMPLE_DIR_PATTERN.matcher(path.toString());
        m.matches();
        return m.group(1);
      }
    };

  private static final PathFilter SKIP_A_PATH_FILTER =
    new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !path.getName().equals("a.txt");
      }
    };

  @Test
  public void testCollectPathsWithDirs() throws Exception {
    List<Path> accumulator = Lists.newLinkedList();
    Path p = new Path(SAMPLE_DIR_LOCATION + "sample_dir");
    HdfsUtils.collectPaths(
      p,
      p.getFileSystem(new Configuration()),
      SKIP_A_PATH_FILTER, accumulator);

    Set<String> expected = Sets.newHashSet(
      "sample_dir",
      "sample_dir/b.txt",
      "sample_dir/nested",
      "sample_dir/nested/c.txt",
      "sample_dir/nested/d.txt",
      "sample_dir/nested/double_nested",
      "sample_dir/nested/double_nested/e.txt");

    Set<String> found = Sets.newHashSet(Iterables.transform(accumulator, PATH_TO_RELATIVE_STRING));

    assertEquals(expected, found);
  }

  @Test
  public  void testCollectPathsWithoutDirs() throws Exception {
    List<Path> accumulator = Lists.newLinkedList();
    Configuration conf = new Configuration();
    Path p = new Path(SAMPLE_DIR_LOCATION + "sample_dir");
    HdfsUtils.collectPaths(
      p,
      p.getFileSystem(conf),
      new PathFilters.CompositePathFilter(
          PathFilters.newExcludeDirectoriesFilter(conf),
          SKIP_A_PATH_FILTER),
      accumulator
    );

    Set<String> expected = Sets.newHashSet(
      "sample_dir/b.txt",
      "sample_dir/nested/c.txt",
      "sample_dir/nested/d.txt",
      "sample_dir/nested/double_nested/e.txt");

    Set<String> found = Sets.newHashSet(Iterables.transform(accumulator, PATH_TO_RELATIVE_STRING));

    assertEquals(expected, found);
  }

  @Test
  public void testGetDirectorySize() throws Exception {
    Path p = new Path(SAMPLE_DIR_LOCATION + "sample_dir");
    long size = HdfsUtils.getDirectorySize(p, p.getFileSystem(new Configuration()));
    assertEquals(18, size);
  }

  @Test
  public void testGetDirectorySizeWithFilter() throws Exception {
    Path p = new Path(SAMPLE_DIR_LOCATION + "sample_dir");
    long size = HdfsUtils.getDirectorySize(
        p,
        p.getFileSystem(new Configuration()),
        PathFilters.ACCEPT_ALL_PATHS_FILTER);
    assertEquals(18, size);

    size = HdfsUtils.getDirectorySize(
      p,
      p.getFileSystem(new Configuration()),
      SKIP_A_PATH_FILTER);
    assertEquals(16, size);
  }

  @Test
  public void testExpandGlobs() throws Exception {
    List<Path> paths = HdfsUtils.expandGlobs(
      Lists.newArrayList(SAMPLE_DIR_LOCATION + "sample_dir/*.txt"), new Configuration());

    assertEquals(Lists.newArrayList("sample_dir/a.txt", "sample_dir/b.txt"),
      Lists.transform(paths, PATH_TO_RELATIVE_STRING));

    paths = HdfsUtils.expandGlobs(
      Lists.newArrayList(SAMPLE_DIR_LOCATION + "sample_dir/a.txt"), new Configuration());

    assertEquals(Lists.newArrayList("sample_dir/a.txt"),
      Lists.transform(paths, PATH_TO_RELATIVE_STRING));

    paths = HdfsUtils.expandGlobs(
      Lists.newArrayList(SAMPLE_DIR_LOCATION + "sample_dir/*.txt",
                         SAMPLE_DIR_LOCATION + "sample_dir/*nes*/*.txt"), new Configuration());

    assertEquals(Lists.newArrayList(
      "sample_dir/a.txt",
      "sample_dir/b.txt",
      "sample_dir/nested/c.txt",
      "sample_dir/nested/d.txt"),
      Lists.transform(paths, PATH_TO_RELATIVE_STRING));
  }

}
