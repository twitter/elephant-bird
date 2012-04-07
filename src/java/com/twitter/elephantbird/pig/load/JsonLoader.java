package com.twitter.elephantbird.pig.load;

import com.google.common.collect.Maps;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Decodes each line as JSON passes the resulting map of values
 * to Pig as a single-element tuple.
 * 
 * <p>This Loader supports loading of nested JSON structures, but this feature
 * is disabled by default. There are two ways to enable it:<ul>
 * <li>setting the -nestedLoadEnabled option in the 
 * {@link JsonLoader#JsonLoader(String, String)} constructor
 * <li>setting the <code>elephantbird.jsonloader.nestedLoadEnabled</code>
 * property to true. This can be done with the following pig command:
 * <pre>grunt> set elephantbird.jsonloader.nestedLoadEnabled 'true'</pre>
 * </ul>
 */
public class JsonLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(JsonLoader.class);
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final BagFactory bagFactory = DefaultBagFactory.getInstance();
  
  public static final String NESTED_ENABLED_KEY = "elephantbird.jsonloader.nestedLoadEnabled";
  
  private final static Options validOptions_ = new Options();
  private final static CommandLineParser parser_ = new GnuParser();
  private final CommandLine configuredOptions_;

  private final JSONParser jsonParser = new JSONParser();

  private enum JsonLoaderCounters {
    LinesRead,
    LinesJsonDecoded,
    LinesParseError,
    LinesParseErrorBadNumber
  }

  private String inputFormatClassName;
  private boolean isNestedLoadEnabled = false;
  private Set<String> fields = null;
  
  private static void populateValidOptions() {
    validOptions_.addOption("nestedLoadEnabled", false, "Enables loading of " +
        "nested JSON structures");
    validOptions_.addOption("fieldsSpec", true, "Fields specification");
  }

  public JsonLoader() {
    // defaults to no fields specification
    this(TextInputFormat.class.getName(), "");
  }

  /**
   * Constructor. Construct a JsonLoader LoadFunc to load. 
   * @param inputFormatClassName the inputFormat class name
   * 
   * @param optString Loader options. Available options:<ul>
   * <li>-nestedLoadEnabled==(true|false) Enables loading of nested JSON
   * structures. When enabled, JSON objects are loaded as nested Maps 
   * and JSON arrays are loaded as Bags.
   * <li>-fieldsSpec=fields
   *        Fields specification, a string delimited by commas representing
   *        a list of JSON fields.
   *        If specified, only fields specified by this list will be loaded,
   *        otherwise, all fields are loaded. To retrieve the value of a
   *        nested field in the JSON structure, specify all fields to arrive
   *        to that object. E.g., to retrieve the value of the object "baz"
   *        in the structure <code>{"foo":{"bar":{"baz":"x"}}</code>, specify
   *        all <code>"foo,bar,baz"</code> in the fields specification.
   * </ul>
   */
  public JsonLoader(String inputFormatClassName, String optString) {
    this.inputFormatClassName = inputFormatClassName;
    populateValidOptions();
    String[] optsArr = optString.split(" ");
    
    try {
      configuredOptions_ = parser_.parse(validOptions_, optsArr);
    } catch (org.apache.commons.cli.ParseException e) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "[-nestedLoadEnabled] [-fieldsSpec]", validOptions_ );
        throw new RuntimeException(e);
    }
    isNestedLoadEnabled = configuredOptions_.hasOption("nestedLoadEnabled");
    
    String fieldsSpec = configuredOptions_.getOptionValue("fieldsSpec");
    if (fieldsSpec != null && fieldsSpec.length() > 0) {
      fields = new HashSet<String>();
      for(String s: fieldsSpec.split(",")) {
        fields.add(s);
      }
    }
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  @Override
  public Tuple getNext() throws IOException {
    if (reader == null) {
      return null;
    }
    // nested load can be disabled through a pig property
    if ("true".equals(jobConf.get(NESTED_ENABLED_KEY)))
      isNestedLoadEnabled = true;
    try {
      while (reader.nextKeyValue()) {
        Text value = (Text) reader.getCurrentValue();
        incrCounter(JsonLoaderCounters.LinesRead, 1L);
        Tuple t = parseStringToTuple(value.toString());
        if (t != null) {
          incrCounter(JsonLoaderCounters.LinesJsonDecoded, 1L);
          return t;
        }
      }
      return null;

    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
          PigException.REMOTE_ENVIRONMENT, e);
    }
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    try {
      return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
    } catch (InstantiationException e) {
      throw new IOException("Failed creating input format " + inputFormatClassName, e);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed creating input format " + inputFormatClassName, e);
    }
  }

  protected Tuple parseStringToTuple(String line) {
    try {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(line);
      if (jsonObj != null) {
        return tupleFactory.newTuple(walkJson(jsonObj));
      } else {
        // JSONParser#parse(String) may return a null reference, e.g. when
        // the input parameter is the string "null".  A single line with
        // "null" is not valid JSON though.
        LOG.warn("Could not json-decode string: " + line);
        incrCounter(JsonLoaderCounters.LinesParseError, 1L);
        return null;
      }
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      incrCounter(JsonLoaderCounters.LinesParseError, 1L);
      return null;
    } catch (NumberFormatException e) {
      LOG.warn("Very big number exceeds the scale of long: " + line, e);
      incrCounter(JsonLoaderCounters.LinesParseErrorBadNumber, 1L);
      return null;
    } catch (ClassCastException e) {
      LOG.warn("Could not convert to Json Object: " + line, e);
      incrCounter(JsonLoaderCounters.LinesParseError, 1L);
      return null;
    }
  }

  private Object wrap(Object value) {
    
    if (isNestedLoadEnabled && value instanceof JSONObject) {
      return walkJson((JSONObject) value);
    }  else if (isNestedLoadEnabled && value instanceof JSONArray) {
      
      JSONArray a = (JSONArray) value;
      DataBag mapValue = bagFactory.newDefaultBag();
      for (int i=0; i<a.size(); i++) {
        Tuple t = tupleFactory.newTuple(wrap(a.get(i)));
        mapValue.add(t);
      }
      return mapValue;
      
    } else {
      return value != null ? value.toString() : null;
    }
  }

  private Map<String,Object> walkJson(JSONObject jsonObj) {
    Map<String,Object> v = Maps.newHashMap();
    for (Object key: jsonObj.keySet()) {
      if (fields == null || fields.contains(key.toString()))
        v.put(key.toString(), wrap(jsonObj.get(key)));
    }
    return v;
  }
  
}
