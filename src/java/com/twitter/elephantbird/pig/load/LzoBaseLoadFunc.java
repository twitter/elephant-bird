package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import com.hirohanin.elephantbird.Slice;
import com.hirohanin.elephantbird.Slicer;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * This class handles LZO-decoding and slicing input LZO files.  It expects the
 * filenames to end in .lzo, otherwise it assumes they are not compressed and skips them.
 * TODO: Improve the logic to accept a mixture of lzo and non-lzo files.
 */
public abstract class LzoBaseLoadFunc extends LoadFunc implements  Slicer {
  private static final Logger LOG = LoggerFactory.getLogger(LzoBaseLoadFunc.class);

  protected final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();

  // This input stream will be our wrapped LZO-decoding stream.
  //protected BufferedPositionedInputStream is_;
  protected RecordReader is_;
  protected long end_;
  // The load func spec is the load function name (with classpath) plus the arguments.
  protected FuncSpec loadFuncSpec_;
  // Whether our split begins at the beginning of the data in the file.  Generally one can check
  // for offset == 0, but not with LZO's variable-length file header.
  protected boolean beginsAtHeader_ = false;

  // Making accessing Hadoop counters from Pig slightly more convenient.
  private final PigCounterHelper counterHelper_ = new PigCounterHelper();

  /**
   * Construct a new load func.
   */
  public LzoBaseLoadFunc() {
    // By default, the spec is the class being loaded with no arguments.
    setLoaderSpec(getClass(), new String[] {});
  }

  /**
   * Set whether this chunk begins at the beginning of the entire file,
   * just after the LZO file header.
   * @param beginsAtHeader whether this is the first chunk of the file, and so
   *        begins at the LZO file header offset.
   */
  public void setBeginsAtHeader(boolean beginsAtHeader) {
    beginsAtHeader_ = beginsAtHeader;
  }

  /**
   * The important part of the loader -- given a storage object and a location to load, actually
   * compute the splits.  Walks through each LZO file under the given path and attempts to use the
   * .lzo.index file to slice it.
   *
   * @param store the data storage object.
   * @param location the given location to load, e.g. '/tables/statuses/20090815.lzo' when invoked as
   * a = LOAD '/tables/statuses/20090815.lzo' USING ... AS ...;
   */
  public Slice[] slice(DataStorage store, String location) throws IOException {
    LOG.info("LzoBaseLoadFunc::slice, location = " + location);
    List<LzoSlice> slices = Lists.newArrayList();
    // Compute the set of LZO files matching the given pattern.
    List<ElementDescriptor> globbedFiles = globFiles(store, location);

    for (ElementDescriptor file : globbedFiles) {
      // Make sure to slice according to the per-file split characteristics.
      Map<String, Object> fileStats = file.getStatistics();
      long blockSize = (Long)fileStats.get(ElementDescriptor.BLOCK_SIZE_KEY);
      long fileSize = (Long)fileStats.get(ElementDescriptor.LENGTH_KEY);

      LOG.debug("Slicing LZO file at path " + file + ": block size " + blockSize + " and file size " + fileSize);
      slices.addAll(sliceFile(file.toString(), blockSize, fileSize));
    }
    if (slices.size() == 0) {
      throw new PigException("no files found a path "+location);
    }
    LOG.info("Got " + slices.size() + " LZO slices in total.");
    return slices.toArray(new Slice[slices.size()]);
  }

  /**
   * Nothing to do here, since location can be an unexpanded glob.
   * Also, the idea that validate returns void and instead throws an exception to fail validation is ridiculous.
   */
  public void validate(DataStorage store, String location) throws IOException {
  }

  /**
   * Called on the datanodes during the data loading process, this connects a Map job with an input split.
   * @param filename the name of the file whose split is being loaded.
   * @param is the input stream to the file.
   * @param offset the offset within the file (the input stream is pre-positioned here)
   * @param end the end offset of the input split.
   */
  public void bindTo(String filename, BufferedPositionedInputStream is, long offset, long end) throws IOException {
  /*LOG.info("LzoBaseLoadFunc::bindTo, filename = " + filename + ", offset = " + offset + ", and end = " + end);
    LOG.debug("InputStream position is: "+is.getPosition());
    is_ = is;
    end_ = end;

    // Override this to do anything loader-specific to the input stream, etc.
    postBind();
    // Override to do any special syncing for moving to the right point of a new input split.
    skipToNextSyncPoint(beginsAtHeader_);

    LOG.debug("InputStream position after skip is: "+is.getPosition());*/
  }

  /**
   * Override to do anything special after the bindTo function has been called, and before the sync happens.
   */
  public void postBind() throws IOException {
  }

  /**
   * Override to do any special syncing for moving to the right point of a new input split.
   *
   * @param atFirstRecord whether or not this is the first record in the file.  Typically for line-based
   * readers, for example, we want to skip to the next new line at the beginning of an input split because
   * the arbitrary byte offset we're at generally puts us in the middle of a line.  We count on the previous
   * input split to read slightly beyond its offset to the end of the next line to account for this.
   * However, this doesn't hold for the very first record in the file.
   */
  public abstract void skipToNextSyncPoint(boolean atFirstRecord) throws IOException;

  /**
   * Give hints to pig about the output schema -- there are none needed.
   */
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return null;
  }

  /**
   * This seems to always be unimplemented.
   */
  public void fieldsToRead(Schema schema) {
  }

  /**
   * Set the loader spec so any arguments given in the script are tracked, to be reinstantiated by the mappers.
   * @param clazz the class of the load function to use.
   * @param args an array of strings that are fed to the class's constructor.
   */
  protected void setLoaderSpec(Class <? extends LzoBaseLoadFunc> clazz, String[] args) {
    loadFuncSpec_ = new FuncSpec(clazz.getName(), args);
  }

  /**
   * A convenience function for working with Hadoop counter objects from load functions.  The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(String group, String counter, long incr) {
    counterHelper_.incrCounter(group, counter, incr);
  }

  /**
   * A convenience function for working with Hadoop counter objects from load functions.  The Hadoop
   * reporter object isn't always set up at first, so this class provides brief buffering to ensure
   * that counters are always recorded.
   */
  protected void incrCounter(Enum<?> key, long incr) {
    counterHelper_.incrCounter(key, incr);
  }

  /**
   * Called to verify that the stream is readable, i.e. not null and not past the byte offset
   * of the next split.
   * @return true if the input stream is valid and has not yet read past the last byte of the current split.
   */
  protected boolean verifyStream() throws IOException {
	  try {
		  return is_ != null && is_.nextKeyValue();
	  } catch (InterruptedException e) {
		  int errCode = 6018;
		  String errMsg = "Error while reading input";
		  throw new ExecException(errMsg, errCode,
              PigException.REMOTE_ENVIRONMENT, e);
	  }

  }

  /**
   * Given a path, glob all the files underneath it, and return all the LZO files.
   * @param store the data store object
   * @param location the input location glob from the pig script
   * @return the set of LZO files matching the location glob
   */
  private List<ElementDescriptor> globFiles(DataStorage store, String location) throws IOException {
    List<ElementDescriptor> files = Lists.newArrayList();
    List<ElementDescriptor> paths = Lists.newArrayList();
    paths.addAll(Arrays.asList(store.asCollection(location)));

    // Note that paths.size increases through the loop as directories are encountered.
    for (int j = 0; j < paths.size(); j++) {
      ElementDescriptor path = paths.get(j);
      ElementDescriptor fullPath = store.asElement(store.getActiveContainer(), path);
      if (fullPath.systemElement()) {
        // Skip Hadoop's private/meta files.
        continue;
      }

      // If it's a directory, add it to the path and go back to the top.
      try {
        if (fullPath instanceof ContainerDescriptor) {
          for (ElementDescriptor child : (ContainerDescriptor)fullPath) {
            paths.add(child);
          }
          continue;
        }
      } catch (Exception e) {
        // See the corresponding part of PigSlicer.java
        int errCode = 2099;
        String msg = "Problem in constructing LZO slices: " + e.getMessage();
        throw new ExecException(msg, errCode, PigException.BUG, e);
      }

      // It's a file.
      // TODO: make this able to read non-LZO data too.
      if (!fullPath.toString().endsWith(LZO_EXTENSION)) {
        continue;
      }
      files.add(fullPath);
    }

    return files;
  }

  /**
   * Given an individual LZO file, break it into input splits according to its block size,
   * then use the index (if it exists) to massage the offsets to LZO block boundaries
   * to allow the file to be split across mappers.
   * @param filename the name of the file being read
   * @param blockSize the Hadoop block size of the file (usually 64 or 128 MB)
   * @param fileSize the total length of the file.
   * @return a list of LzoSlice objects corresponding to the massaged splits.
   */
  private List<LzoSlice> sliceFile(String filename, long blockSize, long fileSize) throws IOException {
    List<LzoSlice> slices = new ArrayList<LzoSlice>();
    Path filePath = new Path(filename);

    LzoIndex index = LzoIndex.readIndex(FileSystem.get(URI.create(filename), new Configuration()), filePath);
    if (index == null || index.isEmpty()) {
      LOG.info("LzoLoadFunc::sliceFile, file " + filename + " and index is empty or nonexistant");
      // If there is no index, or it couldn't be read, don't split.
      slices.add(new LzoSlice(filename, 0, fileSize, loadFuncSpec_));
    } else if (fileSize == 0) {
      LOG.info("LzoLoadFunc::sliceFile, file " + filename + " and fileSize == 0");
      // Add fileSize empty slice.  This is a total hack to deal with the
      // case where hadoop isn't starting maps for empty arrays of
      // InputSplits.  See PIG-619.  This should be removed
      // once we determine why this is.
      slices.add(new LzoSlice(filename, 0, blockSize, loadFuncSpec_));
    } else {
      // There is an index file.  First create the default file splits based on the blocksize.
      List<FileSplit> splits = new ArrayList<FileSplit>();
      for (long pos = 0; pos < fileSize; pos += blockSize) {
        splits.add(new FileSplit(filePath, pos, Math.min(blockSize, fileSize - pos), null));
      }

      LOG.info("LzoLoadFunc::sliceFile, file " + filename + " with size " + fileSize + " into " + splits.size() + " chunks.");
      for (FileSplit split : splits) {
        // Now massage the default splits to LZO block boundaries.
        long start = split.getStart();
        long end = start + split.getLength();

        long lzoStart = index.alignSliceStartToIndex(start, end);
        long lzoEnd = index.alignSliceEndToIndex(end, fileSize);

        if (lzoStart != LzoIndex.NOT_FOUND  && lzoEnd != LzoIndex.NOT_FOUND) {
          slices.add(new LzoSlice(filename, lzoStart, lzoEnd - lzoStart, loadFuncSpec_));
        }
      }
    }

    return slices;
  }



}
