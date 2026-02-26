package com.twitter.elephantbird.pig.util;

/**
 * A helper class to deal with standard Pig tokens and delimiters.
 */
public class PigTokenHelper {
  public static final byte DEFAULT_RECORD_DELIMITER = '\n';
  public static final byte DEFAULT_FIELD_DELIMITER = '\t';
  public static final String DEFAULT_FIELD_DELIMITER_STRING = "\\t";

  // String constants for each delimiter
  public static final String TUPLE_BEGIN = "(";
  public static final String TUPLE_END = ")";
  public static final String BAG_BEGIN = "{";
  public static final String BAG_END = "}";
  public static final String MAP_BEGIN = "[";
  public static final String MAP_END = "]";
  public static final String MAP_KV = "#";

  /**
   * Parse an input delimiter string, as with PigStorage, and return the byte it represents.
   * @param inputDelimiter the string passed in from the pig script.
   * @return the corresponding byte that will serve as the field separator.
   */
  public static byte evaluateDelimiter(String inputDelimiter) {
    if (inputDelimiter.length() == 1) {
      return inputDelimiter.getBytes()[0];
    } else if (inputDelimiter.length() > 1 && inputDelimiter.charAt(0) == '\\') {
      switch (inputDelimiter.charAt(1)) {
      case 't':
        return (byte)'\t';

      case 'x':
      case 'u':
        return Integer.valueOf(inputDelimiter.substring(2)).byteValue();

      default:
        throw new IllegalArgumentException("Unknown delimiter " + inputDelimiter);
      }
    } else {
      throw new IllegalArgumentException("LzoTokenizedStorage delimeter must be a single character");
    }
  }
}
