package com.twitter.elephantbird.mapred.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.json.simple.parser.JSONParser;

import com.twitter.elephantbird.mapreduce.input.LzoJsonRecordReader;

/**
 * Reads line from an lzo compressed text file, and decodes each line into a json map.
 * Skips lines that are invalid json.
 *
 * WARNING: Does not handle multi-line json input well, if at all.
 * TODO: Fix that, and keep Hadoop counters for invalid vs. valid lines.
 */
@SuppressWarnings("deprecation")
public class DeprecatedLzoJsonRecordReader implements RecordReader<LongWritable, MapWritable> {

  private DeprecatedLzoLineRecordReader textReader;
  private JSONParser jsonParser = new JSONParser();
  private Text curLine = new Text();

  DeprecatedLzoJsonRecordReader(Configuration conf, FileSplit split) throws IOException {
    textReader = new DeprecatedLzoLineRecordReader(conf, split);
  }

  @Override
  public void close() throws IOException {
    textReader.close();
  }

  @Override
  public LongWritable createKey() {
    return textReader.createKey();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return textReader.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return textReader.getProgress();
  }

  @Override
  public boolean next(LongWritable key, MapWritable value) throws IOException {
    // skip lines that fail json parsing.
    while (textReader.next(key, curLine)) {
      if (LzoJsonRecordReader.decodeLineToJson(jsonParser, curLine, value)) {
        return true;
      }
    }
    return false;
  }
}
