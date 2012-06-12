package com.twitter.elephantbird.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrappers around the singularization, pluaralization, etc in Inflection.
 *
 * Largely taken from the defunct rogueweb project at
 * <link>http://code.google.com/p/rogueweb/</link>
 * Original author Anthony Eden.
 */
public class Strings {

  public static String tableize(String word) {
    return pluralize(underscore(depackage(word)));
  }

  /**
   * Pluralize the given word.
   * @param word The word
   * @return The pluralized version of the word
   */
  public static String pluralize(String word) {
    return Inflection.pluralize(word);
  }

  public static String singularize(String word) {
    return Inflection.singularize(word);
  }

  /**
   * Underscore the given word.
   * @param word The word
   * @return The underscored version of the word
   */
  public static String underscore(String word) {
    String firstPattern = "([A-Z]+)([A-Z][a-z])";
    String secondPattern = "([a-z\\d])([A-Z])";
    String replacementPattern = "$1_$2";
    // Replace package separator with slash.
    word = word.replaceAll("\\.", "/");
    // Replace $ with two underscores for inner classes.
    word = word.replaceAll("\\$", "__");
    // Replace capital letter with _ plus lowercase letter.
    word = word.replaceAll(firstPattern, replacementPattern);
    word = word.replaceAll(secondPattern, replacementPattern);
    word = word.replace('-', '_');
    word = word.toLowerCase();
    return word;
  }

  public static String camelize(String word) {
    return camelize(word, false);
  }

  public static String camelize(String word, boolean lowercaseFirstLetter) {
    // Replace all slashes with dots (package separator)
    Pattern p = Pattern.compile("\\/(.?)");
    Matcher m = p.matcher(word);
    while (m.find()) {
      word = m.replaceFirst("." + m.group(1)/*.toUpperCase()*/);
      m = p.matcher(word);
    }

    // Uppercase the class name.
    p = Pattern.compile("(\\.?)(\\w)([^\\.]*)$");
    m = p.matcher(word);
    if (m.find()) {
      String rep = m.group(1) + m.group(2).toUpperCase() + m.group(3);
      rep = rep.replaceAll("\\$", "\\\\\\$");
      word = m.replaceAll(rep);
    }

    // Replace two underscores with $ to support inner classes.
    p = Pattern.compile("(__)(.)");
    m = p.matcher(word);
    while (m.find()) {
      word = m.replaceFirst("\\$" + m.group(2).toUpperCase());
      m = p.matcher(word);
    }

    // Remove all underscores
    p = Pattern.compile("(_)(.)");
    m = p.matcher(word);
    while (m.find()) {
      word = m.replaceFirst(m.group(2).toUpperCase());
      m = p.matcher(word);
    }

    if (lowercaseFirstLetter) {
      word = word.substring(0, 1).toLowerCase() + word.substring(1);
    }

    return word;
  }

  public static String depackage(String word) {
    return word.replaceAll("^.*\\.", "");
  }

  public static String foreignKey(String className) {
    return foreignKey(className, true);
  }

  public static String foreignKey(String className, boolean separateIdWithUnderscore) {
    return underscore(depackage(className)) + (separateIdWithUnderscore ? "_id" : "id");
  }

  public static String titleize(String word) {
    word = humanize(underscore(word));
    Pattern p = Pattern.compile("\\b([a-z])");
    Matcher m = p.matcher(word);
    while (m.find()) {
      word = m.replaceFirst(capitalize(m.group(1)));
      m = p.matcher(word);
    }
    return word;
  }

  /**
   * Converts a table name into a class name.
   * @param tableName The table name
   * @return The class name
   */
  public static String classify(String tableName) {
    return camelize(singularize(tableName.replaceAll(".*\\.", "")));
  }

  public static String humanize(String word) {
    word = word.replaceAll("_id", "");
    return capitalize(word.replaceAll("_", " "));
  }

  /**
   * Return the ordinal for the given number.
   * @param n The number
   * @return The ordinal String
   */
  public static String ordinalize(int n) {
    int mod100 = n % 100;
    if (mod100 == 11 || mod100 == 12 || mod100 == 13) {
      return String.valueOf(n) + "th";
    }
    switch (n % 10) {
      case 1:
        return String.valueOf(n) + "st";
      case 2:
        return String.valueOf(n) + "nd";
      case 3:
        return String.valueOf(n) + "rd";
      default:
        return String.valueOf(n) + "th";
    }
  }

  /**
   * Replace underscores with dashes.
   * @param word The word
   * @return The word with underscores converted to dashes
   */
  public static String dasherize(String word) {
    return word.replaceAll("_", "-");
  }

  public static String capitalize(String word) {
    word = word.toLowerCase();
    word = word.substring(0, 1).toUpperCase() + word.substring(1);
    return word;
  }
}
