package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import com.twitter.elephantbird.util.W3CLogParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader for LZO-compressed W3C-style log formatted files.  See discussion in the
 * LzoW3CLogInputFormat for more on the format.  To use, derive from this class and implement
 * the getFieldDefinitionFile() method. <br>
 * Most commonly, you can simply call LzoW3CLogInputFormat.newInstance(myFilePath)
 */
public abstract class LzoW3CLogRecordReader extends LzoRecordReader<LongWritable, MapWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoW3CLogRecordReader.class);

  private LineReader in_;

  private final LongWritable key_ = new LongWritable();
  protected final Text currentLine_ = new Text();
  private final MapWritable value_ = new MapWritable();
  protected W3CLogParser w3cLogParser_ = null;

  // Used to hold the number of unparseable records seen between successfull readings.
  private int badRecordsSkipped_ = 0;

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

    String fileURI = getFieldDefinitionFile();
    Path path = new Path(fileURI);
    InputStream is = path.getFileSystem(conf).open(path);
    w3cLogParser_ = new W3CLogParser(is);
    is.close();
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      in_.readLine(new Text());
    }
  }

  public long getBadRecordsSkipped() {
    return badRecordsSkipped_;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    value_.clear();
    badRecordsSkipped_ = 0;
    while (pos_ <= end_) {
      key_.set(pos_);

      int newSize = in_.readLine(currentLine_);
      if (newSize == 0) {
        return false;
      }

      pos_ = getLzoFilePos();

      if (!decodeLine()) {
        badRecordsSkipped_ += 1;
        continue;
      }

      return true;
    }

    return false;
  }

  protected boolean decodeLine() {
    return decodeLine(currentLine_.toString());
  }

  protected boolean decodeLine(String str) {
    try {
      Map<String, String> w3cLogFields = w3cLogParser_.parse(str);
      for(Map.Entry<String, String> entrySet : w3cLogFields.entrySet()) {
        String value = entrySet.getValue();
        value_.put(new Text(entrySet.getKey()), new Text(value));
      }
      return true;
    } catch (IOException e) {
      LOG.debug("Could not w3c-decode string: " + str, e);
      return false;
    }
  }

  protected abstract String getFieldDefinitionFile();
}

