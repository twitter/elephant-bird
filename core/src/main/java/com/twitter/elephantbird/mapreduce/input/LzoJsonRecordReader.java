package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads line from an lzo compressed text file, and decodes each line into a json map.
 * Skips lines that are invalid json.
 *
 * WARNING: Does not handle multi-line json input well, if at all.
 * TODO: Fix that, and keep Hadoop counters for invalid vs. valid lines.
 */
public class LzoJsonRecordReader extends LzoRecordReader<LongWritable, MapWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoJsonRecordReader.class);

  private LineReader in_;

  private final LongWritable key_ = new LongWritable();
  private final Text currentLine_ = new Text();
  private final MapWritable value_ = new MapWritable();
  private final JSONParser jsonParser_ = new JSONParser();

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (in_ != null) {
      in_.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public MapWritable getCurrentValue() throws IOException, InterruptedException {
    return value_;
  }

  @Override
  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    in_ = new LineReader(input, conf);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      in_.readLine(new Text());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    value_.clear();

    while (pos_ <= end_) {
      key_.set(pos_);

      int newSize = in_.readLine(currentLine_);
      if (newSize == 0) {
        return false;
      }

      pos_ = getLzoFilePos();

      if (!decodeLineToJson(jsonParser_, currentLine_, value_)) {
        continue;
      }

      return true;
    }

    return false;
  }

  public static boolean decodeLineToJson(JSONParser parser, Text line, MapWritable value) {
    try {
      JSONObject jsonObj = (JSONObject)parser.parse(line.toString());
      if (jsonObj != null) {
        for (Object key: jsonObj.keySet()) {
          Text mapKey = new Text(key.toString());
          Text mapValue = new Text();
          if (jsonObj.get(key) != null) {
            mapValue.set(jsonObj.get(key).toString());
          }

          value.put(mapKey, mapValue);
        }
      }
      else {
          // JSONParser#parse(String) may return a null reference, e.g. when
          // the input parameter is the string "null".  A single line with
          // "null" is not valid JSON though.
          LOG.warn("Could not json-decode string: " + line);
          return false;
      }
      return true;
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      return false;
    } catch (NumberFormatException e) {
      LOG.warn("Could not parse field into number: " + line, e);
      return false;
    }
  }
}

