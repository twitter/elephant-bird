package com.twitter.elephantbird.pig.load;

import java.util.regex.Pattern;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LzoRegexLoader extends LzoBaseRegexLoader, allowing regular expressions to be passed by argument through pig latin
 * via a line like:
 * A = LOAD 'test.txt' USING com.twitter.elephantbird.pig.storage.LzoRegexLoader('(\\d+)::(\\w+)\|\|(\\w+)');
 * which would parse lines like
 * 1::one||i 2::two||ii 3::three--iii
 * into arrays like
 * {1, "one", "i"}, {2, "two", "ii"}, {3, "three", "iii"}
 */
public class LzoRegexLoader extends LzoBaseRegexLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LzoRegexLoader.class);

  private final Pattern pattern_;

  /**
   * The regex is passed in via the constructor.
   * @param pattern the regex.
   */
  public LzoRegexLoader(String pattern) {
    LOG.info("LzoRegexLoader with regex = " + pattern);

    pattern = pattern.replace("\\\\","\\");
    pattern_ = Pattern.compile(pattern);
  }

  /**
   * Implement the abstract part of the class by returning the pattern.
   */
  @Override
  public Pattern getPattern() {
    return pattern_;
  }
}
