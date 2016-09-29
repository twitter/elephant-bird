package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.pig.data.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 * A storage class to store the ouput of each tuple in a delimited file
 * like PigStorage, but LZO compressed.
 */
public class JsonStorage extends BaseStoreFunc {
    private static final Logger LOG = LoggerFactory.getLogger(JsonStorage.class);

  // If null, keep all keys.
  private final Set<String> keysToKeep_;
  private final JSONObject json = new JSONObject();

  public JsonStorage() {
    keysToKeep_ = null;
    LOG.info("Initialized JsonStorage. Keeping all keys.");
  }

  public JsonStorage(String...keysToKeep) {
    keysToKeep_ = Sets.newHashSet(keysToKeep);
    LOG.info("Initialized JsonStorage. Keeping keys " + Joiner.on(", ").join(keysToKeep_) + ".");
  }


  /**
   * The first element is expected to be a map, or null. Anything else causes an error.
   * @param tuple the tuple to write.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void putNext(Tuple tuple) throws IOException {
    json.clear();
    if (tuple != null && tuple.size() >= 1) {
      Map<String, Object> map = (Map<String, Object>) tuple.get(0);
      
      if(map != null) {
	  System.err.println("YOSAN: map = " + map);
	  for (Map.Entry<String, Object> entry : map.entrySet()) {
	      System.err.println("  YOSAN: key, value = " + entry.getKey() + "," + entry.getValue());
	      if(entry.getValue().toString().startsWith("[")) {
		  //unpack and pack :)
		  JSONArray val = (JSONArray) JSONValue.parse(entry.getValue().toString());
		  //List<String> val = (List<String>) entry.getValue();
		  json.put(entry.getKey(), val);
	      } else {
		  json.put(entry.getKey(), entry.getValue());
	      }
	  }
	  try {
	      writer.write(null, new Text(json.toString()));
	  } catch (InterruptedException e) {
	      // Under what circumstances does this happen?
	      throw new IOException(e);
	  }
      }
    }
  }


  @Override
  public OutputFormat<NullWritable, Text> getOutputFormat() {
      return new TextOutputFormat<org.apache.hadoop.io.NullWritable,org.apache.hadoop.io.Text>();
  }


}

