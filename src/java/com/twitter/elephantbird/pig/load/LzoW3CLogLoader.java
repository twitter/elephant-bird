package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;

import com.twitter.elephantbird.util.W3CLogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Load w3c log LZO file line by line, passing each line as a single-field Tuple to Pig.
 */
public class LzoW3CLogLoader extends LzoBaseLoadFunc {
  protected static final Logger LOG = LoggerFactory.getLogger(LzoW3CLogLoader.class);

  protected static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte RECORD_DELIMITER = (byte)'\n';

  protected W3CLogParser w3cParser_ = null;

  protected enum LzoW3CLogLoaderCounters { LinesRead, LinesW3CDecoded };

  public LzoW3CLogLoader(String fileURI) throws IOException {
    LOG.info("Initialize LzoW3CLogLoader from " + fileURI);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(fileURI), conf);
    InputStream fieldDefIs = fs.open(new Path(fileURI));
    w3cParser_ = new W3CLogParser(fieldDefIs);
    fieldDefIs.close();
  }

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first record of each split and count on a different
    // instance to read it.  The only split this doesn't work for is the first.
    if (!atFirstRecord) {
      getNext();
    }
  }

  /**
   * Return every non-null line as a single-element tuple to Pig.
   */
  public Tuple getNext() throws IOException {
    if (!verifyStream()) {
      return null;
    }

    String line;
    while( (line = is_.readLine(UTF8, RECORD_DELIMITER)) != null) {
      incrCounter(LzoW3CLogLoaderCounters.LinesRead, 1L);

      Tuple t = parseStringToTuple(line);
      if (t != null) {
        incrCounter(LzoW3CLogLoaderCounters.LinesW3CDecoded, 1L);
        return t;
      }
    }
    return null;
  }

  protected Tuple parseStringToTuple(String line) {
    try {
      return tupleFactory_.newTuple(w3cParser_.parse(line));
    } catch (IOException e) {
      // Remove this for now because our logs have a lot of unmatched records
//      LOG.warn("Could not w3c-decode string: " + line, e);
      return null;
    }
  }

  @Override
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return new Schema(new FieldSchema("data", DataType.MAP));
  }
}
