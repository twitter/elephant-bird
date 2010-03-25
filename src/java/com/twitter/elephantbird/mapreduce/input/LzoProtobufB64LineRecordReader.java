package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads line from an lzo compressed text file, base64 decodes it, and then
 * deserializes that into the templatized protobuf object.  Returns <position, protobuf>
 * pairs.
 */
public class  LzoProtobufB64LineRecordReader<M extends Message, W extends ProtobufWritable<M>> extends LzoRecordReader<LongWritable, W> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufBlockRecordReader.class);

  private LineReader lineReader_;

  private final Text line_ = new Text();
  private final LongWritable key_ = new LongWritable();
  private final W value_;
  private final TypeRef<M> typeRef_;
  private final Base64 base64_ = new Base64();

  public LzoProtobufB64LineRecordReader(TypeRef<M> typeRef, W protobufWritable) {
    typeRef_ = typeRef;
    LOG.info("LzoProtobufBlockRecordReader, type args are " + typeRef_.getRawClass());
    value_ = protobufWritable;
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
    return value_;
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
      M protoValue = Protobufs.<M>parseFrom(typeRef_.getRawClass(), base64_.decode(lineBytes));
      if (protoValue == null) {
        continue;
      }

      value_.set(protoValue);
      return true;
    }

    return false;
  }
}
