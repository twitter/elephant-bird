package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;

/**
 * @deprecated replaced by {@link HBaseStorage}.
 */
@Deprecated
public class HBaseLoader extends HBaseStorage {
  private static final Log LOG = LogFactory.getLog(HBaseLoader.class);

  static private void warn() {
    LOG.warn("HBaseLoader is deprecated and will be removed soon."
        + " Please use " + HBaseStorage.class.getName()
        + ". HBaseStorage is a drop in replacement.");
  }

  public HBaseLoader(String columnList) throws ParseException, IOException {
    super(columnList);
    warn();
  }

  public HBaseLoader(String columnList, String optString) throws ParseException, IOException {
    super(columnList, optString);
    warn();
  }
}
