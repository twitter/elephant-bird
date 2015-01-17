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

    final Configuration config = conf;
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
        try {
          super.close();

          if ( isIndexed ) {
            // rename or remove the index file based on file size.
            Path tmpPath = getIndexFilePath();
            FileStatus stat = getFileStatus(file, config);
            if (stat.getLen() <= stat.getBlockSize()) {
              fs.delete(tmpPath, false);
            } else {
              fs.rename(tmpPath, file.suffix(LzoIndex.LZO_INDEX_SUFFIX));
            }
          }
        } catch(IOException e) {
          // cleanup any output, as S3 file system sometimes fails with
          // partial output saved, killing downstream retries of the task
          deleteOutput();
          throw e;
        }
      }
      
      private Path getIndexFilePath() {
          return file.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
      }
      
      /**
       * Delete the output file and index file, if they exist.
       */
      private void deleteOutput() {
          // delete main data output file, if it exists
          try {
            // try to get the file with retries to allow
            // for S3 eventual consistency.
            // will throw IOException if cannot reach file
            getFileStatus(file, config);
            fs.delete(file, false);
          } catch(IOException e) {
            LOG.warn("Unable to delete file" + file + ", continuing.", e);
          }
          
          if (isIndexed) {
            Path indexFilePath = getIndexFilePath();
            try {
                // try to get the file with retries to allow
                // for eventual consistency
                getFileStatus(indexFilePath, config);
                fs.delete(indexFilePath, false);
              } catch(IOException e) {
                LOG.warn("Unable to delete file" + indexFilePath + ", continuing.", e);
              }
          }
      }
      
      /**
       * Get the status of a file with retries in case of errors.  This is useful
       * for the S3 file system, where eventual consistency can cause files to not appear
       * for a few seconds, which would otherwise cause the task to fail, and then future
       * tasks to fail when the file already exists. 
       * @param conf
       * @return
     * @throws IOException 
       */
      private FileStatus getFileStatus(Path filePath, Configuration conf) throws IOException {
          int numRetriesRemaining = 
                  conf.getInt("elephantbird.lzo.output.index.retries", 20);
          int retrySleepMs = 
                  conf.getInt("elephantbird.lzo.output.index.retries.sleep", 1000);
          do {
              try {
                  return fs.getFileStatus(filePath);
              } catch (IOException e) {
                  if (numRetriesRemaining <= 0) {
                      throw e;
                  } else {
                      LOG.warn("Exception trying to get status of path " + filePath + "-  Retrying.", e);
                      numRetriesRemaining -= 1;
                  }
                  
                  try {
                    Thread.sleep(retrySleepMs);
                } catch (InterruptedException e1) {
                    throw new IOException(e1);
                }
              }
          } while (true);
      }
    };
  }

}
