package com.twitter.elephantbird.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A parser for W3C-style log lines.  See LzoW3CLogInputFormat for more details about
 * the format itself.  Create the parser with an InputStream open to a list of
 * hash/field-list lines that give all the field-sets ever used for logging.  That is,
 * if you logged a set of fields for two days, then added a parameter and logged that set of
 * fields for a day, then added another parameter and removed a different one and logged
 * that set of fields for a while, the field definition file should have three lines
 * with the hashes and corresponding set of fields for each.
 */
public class W3CLogParser {
  protected static final Logger LOG = LoggerFactory.getLogger(W3CLogParser.class);

  private Map<String, List<String>> fieldDef_ = Maps.newHashMap();
  private final static String DELIMITER = "\\s+";

  // Initialize a W3CLogParser From a InputStream
  public W3CLogParser(InputStream is) throws IOException {
    BufferedReader buf = new BufferedReader(new InputStreamReader(is));
    String line;
    while ((line = buf.readLine()) != null) {
      List<String> fields = Arrays.asList(line.split(DELIMITER));
      fieldDef_.put(fields.get(0), fields.subList(1, fields.size()));
    }
  }

  // Parse a line using the field definition, and put results into a Map of
  // <FieldName, Value>
  public Map<String, String> parse(String line) throws IOException {
    Map<String, String> map = new HashMap<String, String>();

    // Get the version CRC and find the field definition
    List<String> fields = Arrays.asList(line.split(DELIMITER));
    List<String> fieldNames = fieldDef_.get(fields.get(0));
    if (fieldNames == null) {
      throw new IOException("cannot find matching field definition for CRC "
          + fields.get(0));
    }
    if (fieldNames.size() != fields.size()) {
      throw new IOException("W3C field definition and input line for CRC "
          + fields.get(0) + " does not match:\n" + line);
    }

    // Map values to field names
    for (int fieldNum = 1; fieldNum < fieldNames.size(); fieldNum++) {
      map.put(fieldNames.get(fieldNum), fields.get(fieldNum));
    }

    return map;
  }
}
