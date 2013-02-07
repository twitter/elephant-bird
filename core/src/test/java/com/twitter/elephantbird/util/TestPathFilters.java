package com.twitter.elephantbird.util;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Alex Levenson
 */
public class TestPathFilters {
  private static final Map<String, Boolean> HIDDEN_EXPECTED = ImmutableMap.<String, Boolean>builder()
    .put("/a/hidden/.path", false)
    .put("/another/hidden/_path", false)
    .put(".hidden", false)
    .put("_hidden", false)
    .put("not/hidden/a.txt", true)
    .put("not_hidden.txt", true)
    .build();

  private static final Map<String, Boolean> ALWAYS_EXPECTED = ImmutableMap.<String, Boolean>builder()
    .put("/a/hidden/.path", true)
    .put("/another/hidden/_path", true)
    .put(".hidden", true)
    .put("_hidden", true)
    .put("not/hidden/a.txt", true)
    .put("not_hidden.txt", true)
    .build();

  @Test
  public void testExcludeHiddenPathFilter() {
    doTestPathFilter(HIDDEN_EXPECTED, PathFilters.EXCLUDE_HIDDEN_PATHS_FILTER);
  }

  @Test
  public void testAcceptAllPathsFilter() {
    doTestPathFilter(ALWAYS_EXPECTED, PathFilters.ACCEPT_ALL_PATHS_FILTER);
  }

  @Test
  public void testCompositePathFilterOnlyDelegate() {
    PathFilter f = new PathFilters.CompositePathFilter(PathFilters.EXCLUDE_HIDDEN_PATHS_FILTER);
    doTestPathFilter(HIDDEN_EXPECTED, f);
    f = new PathFilters.CompositePathFilter(PathFilters.ACCEPT_ALL_PATHS_FILTER);
    doTestPathFilter(ALWAYS_EXPECTED, f);
  }

  @Test
  public void testCompositePathFilter() {
    Map<String, Boolean> expected = ImmutableMap.<String, Boolean>builder()
      .put("/this/is/a/path", false)
      .put("a_file", true)
      .put("/look/another/path", false)
      .put("_hidden", false)
      .put("/this/is/.hidden", false)
      .put("a.txt", false)
      .put("/a/b.txt", true)
      .build();
    PathFilter f = new PathFilters.CompositePathFilter(
      PathFilters.EXCLUDE_HIDDEN_PATHS_FILTER,
      new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return !path.toString().contains("path");
        }
      },
      new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return !path.toString().contains("a.txt");
        }
      }
    );
    doTestPathFilter(expected, f);
  }

  private void doTestPathFilter(Map<String, Boolean> expected, PathFilter filter) {
    for (Map.Entry<String, Boolean> e : expected.entrySet()) {
      assertEquals(e.getValue(), filter.accept(new Path(e.getKey())));
    }
  }
}
