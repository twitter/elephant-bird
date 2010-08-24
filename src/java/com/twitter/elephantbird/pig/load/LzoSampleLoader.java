package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.SeekableInputStream;
import org.apache.pig.backend.datastorage.SeekableInputStream.FLAGS;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.RandomSampleLoader;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.pig.util.LzoBufferedPositionedInputStream;

public class LzoSampleLoader extends RandomSampleLoader {
  private static final Logger LOG = LoggerFactory.getLogger(LzoSampleLoader.class);
  private final String innerFuncSpec;
  private DataStorage store_;

  public LzoSampleLoader(String funcSpec, String ns) {
    super(funcSpec, ns);
    innerFuncSpec = funcSpec;
    loader = (LoadFunc)PigContext.instantiateFuncFromSpec(funcSpec);
  }

  public void bindTo(String fileName, BufferedPositionedInputStream is,
      long offset, long end) throws IOException {
   //skipInterval = (end - offset)/numSamples;
    SeekableInputStream fsis = store_.asElement(store_.getActiveContainer(), fileName).sopen();

    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(new Configuration());
    final CompressionCodec codec = compressionCodecs.getCodec(new Path(fileName));
    CompressionInputStream is_ = codec.createInputStream(fsis, codec.createDecompressor());
    // At this point, is_ will already be a nonzero number of bytes into the file, because
    // the Lzop codec reads the header upon opening the stream.
    boolean beginsAtHeader = false;
    long length = end-offset;
    if (offset != 0) {
      // If offset is nonzero, seek there to begin reading, using SEEK_SET per above.
      fsis.seek(offset, FLAGS.SEEK_SET);
    } else {
      // If offset is zero, then it's actually at the header offset. Adjust based on this.
      offset = fsis.tell();
      length -= offset;
      beginsAtHeader = true;
    }

    ((LzoBaseLoadFunc) loader).setBeginsAtHeader(beginsAtHeader);
    // Wrap Pig's BufferedPositionedInputStream with our own, which gives positions based on the number
    // of compressed bytes read rather than the number of uncompressed bytes read.
    //loader.bindTo(fileName, new LzoBufferedPositionedInputStream(is_, offset), offset, offset + length);
  }


  public void validate(DataStorage store, String location) throws IOException {
    store_ = store;
    ((LzoBaseLoadFunc) loader).validate(store, location);
  }


  public void setStorage(DataStorage base) {
    store_ = base;
  }

}
