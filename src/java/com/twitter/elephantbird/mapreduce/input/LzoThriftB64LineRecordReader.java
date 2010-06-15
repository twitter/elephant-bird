package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.twitter.elephantbird.mapreduce.input.LzoRecordReader;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephantbird.mapreduce.io.ThriftB64LineWritable;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads line from an lzo compressed text file, base64 decodes it, and then
 * deserializes that into the templatized thrift writable. Returns <position, thrift>
 * pairs
 */
public class  LzoThriftB64LineRecordReader<T extends TBase, W extends ThriftB64LineWritable<T>>
    extends LzoRecordReader<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftB64LineRecordReader.class);

  private LineReader lineReader_;

  private final Text line_ = new Text();
  private final LongWritable key_ = new LongWritable();
  private final TypeRef<T> typeRef_;
  private final W thriftWritable_;
  private final Base64 base64_ = new Base64();
  private final TDeserializer deserializer_ = new TDeserializer();

  public LzoThriftB64LineRecordReader(TypeRef<T> typeRef, W thriftWritable) {
    typeRef_ = typeRef;
    thriftWritable_ = thriftWritable;
    LOG.info("LzoProtobufBlockRecordReader, type args are " + typeRef_.getRawClass());
  }

  @Override
  public synchronized void close() throws IOException {
    if (lineReader_ != null) {
      lineReader_.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key_;
  }

  @Override
  public W getCurrentValue() throws IOException, InterruptedException {
    return thriftWritable_;
  }

  @Override
  protected void createInputReader(InputStream input, Configuration conf) throws IOException {
    lineReader_ = new LineReader(input, conf);
  }

  @Override
  protected void skipToNextSyncPoint(boolean atFirstRecord) throws IOException {
    if (!atFirstRecord) {
      lineReader_.readLine(new Text());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Since the lzop codec reads everything in lzo blocks, we can't stop if pos == end.
    // Instead we wait for the next block to be read in, when pos will be > end.
    while (pos_ <= end_) {
      key_.set(pos_);

      int newSize = lineReader_.readLine(line_);
      if (newSize == 0) {
        return false;
      }
      pos_ = getLzoFilePos();
      byte[] lineBytes = line_.toString().getBytes("UTF-8");
      T thriftValue = typeRef_.safeNewInstance();
      try {
        deserializer_.deserialize(thriftValue, base64_.decode(lineBytes));
      }
      catch (TException e) {
        // TODO: increment counter.
        continue;
      }

      thriftWritable_.set(thriftValue);
      return true;
    }

    return false;
  }
}
