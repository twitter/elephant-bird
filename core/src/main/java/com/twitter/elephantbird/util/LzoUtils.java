package com.twitter.elephantbird.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * Miscellaneous lzo related utilities.
 */
public class LzoUtils {

  public static final Logger LOG = LoggerFactory.getLogger(LzoUtils.class);

  /**
   * A work-around to support environments with older versions of LzopCodec.
   * It might not be feasible for to select right version of hadoop-lzo
   * in some cases. This should be removed latest by EB-3.0.
   */
  private static boolean isLzopIndexSupported = false;
  static {
    try {
      isLzopIndexSupported =
        null != LzopCodec.class.getMethod("createIndexedOutputStream",
                                          OutputStream.class,
                                          DataOutputStream.class);
    } catch (Exception e) {
      // older version of hadoop-lzo.
    }
  }

  /**
   * Creates an lzop output stream. The index for the lzop is
   * also written to another at the same time if
   * <code>elephantbird.lzo.output.index</code> is set in configuration. <p>
   *
   * If the file size at closing is not larger than a single block,
   * the index file is deleted (in line with {@link LzoIndexer} behavior).
   */
  public static DataOutputStream
  getIndexedLzoOutputStream(Configuration conf, Path path) throws IOException {

    LzopCodec codec = new LzopCodec();
    codec.setConf(conf);

    final Path file = path;
    final FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);

    FSDataOutputStream indexOut = null;
    if (conf.getBoolean("elephantbird.lzo.output.index", false)) {
      if ( isLzopIndexSupported ) {
        Path indexPath = file.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
        indexOut = fs.create(indexPath, false);
      } else {
        LOG.warn("elephantbird.lzo.output.index is enabled, but LzopCodec "
            + "does not have createIndexedOutputStream method. "
            + "Please upgrade hadoop-lzo.");
      }
    }

    final boolean isIndexed = indexOut != null;

    OutputStream out = ( isIndexed ?
        codec.createIndexedOutputStream(fileOut, indexOut) :
        codec.createOutputStream(fileOut) );

    return new DataOutputStream(out) {
      // override close() to handle renaming index file.

      public void close() throws IOException {
        super.close();

        if ( isIndexed ) {
          // rename or remove the index file based on file size.

          Path tmpPath = file.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
          FileStatus stat = fs.getFileStatus(file);
          if (stat.getLen() <= stat.getBlockSize()) {
            fs.delete(tmpPath, false);
          } else {
            fs.rename(tmpPath, file.suffix(LzoIndex.LZO_INDEX_SUFFIX));
          }
        }
      }
    };
  }

}
