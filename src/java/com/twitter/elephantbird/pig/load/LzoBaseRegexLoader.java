package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves as the base class for all regex-based lzo loading functions.  Each deriving
 * class just needs to implement getPattern in some way.
 */

public abstract class LzoBaseRegexLoader extends LzoBaseLoadFunc {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseRegexLoader.class);

  protected static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
  protected static final Charset UTF8 = Charset.forName("UTF-8");
  protected static final byte RECORD_DELIMITER = (byte)'\n';

  // This loader tracks the number of matched and unmatched lines as Hadoop counters.
  protected enum LzoBaseRegexLoaderCounters { MatchedRegexLines, UnmatchedRegexLines }

  public LzoBaseRegexLoader() {
    LOG.info("LzoBaseRegexLoader created.");
  }

  /**
   * The regex pattern must be filled out by the inheritor.
   */
  public abstract Pattern getPattern();

  public void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    // Since we are not block aligned we throw away the first record of each split and count on a different
    // instance to read it.  The only split this doesn't work for is the first.
    if (!atFirstRecord) {
      getNext();
    }
  }

  /**
   * Read the file line by line, returning lines the match the regex in Tuples
   * based on the regex match groups.
   */
  public Tuple getNext() throws IOException {
    if (!verifyStream()) {
      return null;
    }

    Pattern pattern = getPattern();
    Matcher matcher = pattern.matcher("");
    String line;
    Tuple t = null;
    // Read lines until a match is found, making sure there's no reading past the
    // end of the assigned byte range.
    while (true) {
      line = is_.readLine(UTF8, RECORD_DELIMITER);

      if (line == null) {
        break;
      }

      matcher = matcher.reset(line);
      // Increment counters for the number of matched and unmatched lines.
      if (matcher.find()) {

        incrCounter(LzoBaseRegexLoaderCounters.MatchedRegexLines, 1L);
        t = tupleFactory_.newTuple(matcher.groupCount());
        for (int i = 1; i <= matcher.groupCount(); i++) {
          if(matcher.group(i) != null) {
            t.set(i - 1, matcher.group(i));
          } else {
            t.set(i - 1, "");
          }
        }
        break;
      } else {
        incrCounter(LzoBaseRegexLoaderCounters.UnmatchedRegexLines, 1L);
        // TODO: stop doing this, as it can slow down the job.
        LOG.debug("No match for line " + line);
      }

      // If the read has walked beyond the end of the split, move on.
      if (is_.getPosition() > end_) {
        break;
      }
    }

    return t;
  }
}
