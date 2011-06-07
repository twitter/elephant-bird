package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoLineRecordReader;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.thrift.TBase;

import java.io.IOException;

/**
 * Decode a lzo compressed line, apply b64 decoding then deserialize into
 * prescribed thrift object, will skip empty line and undecodable line.
 *
 * @author Yifan SHi
 */
@SuppressWarnings("deprecation")
public class DeprecatedLzoThriftB64LineRecordReader<M extends TBase<?, ?>>
    implements RecordReader<LongWritable, ThriftWritable<M>> {
  private DeprecatedLzoLineRecordReader textReader;

  private TypeRef<M> typeRef_;
  private final ThriftWritable<M> value_;

  private final Base64 base64_ = Codecs.createStandardBase64();
  private final ThriftConverter<M> converter_;


  public DeprecatedLzoThriftB64LineRecordReader(
      Configuration conf, FileSplit split, TypeRef<M> typeRef) throws IOException {
    textReader = new DeprecatedLzoLineRecordReader(conf, split);
    typeRef_ = typeRef;
    converter_ = new ThriftConverter<M>(typeRef);
    value_ = new ThriftWritable<M>(typeRef);
  }

  public void close() throws IOException {
    textReader.close();
  }

  public LongWritable createKey() {
    return textReader.createKey();
  }


  public ThriftWritable<M> createValue() {
    return new ThriftWritable<M>();
  }

  public long getPos() throws IOException {
    return textReader.getPos();
  }

  public float getProgress() throws IOException {
    return textReader.getProgress();
  }

  public boolean next(LongWritable key, ThriftWritable<M> value) throws IOException {
    Text text = new Text();
    while (textReader.next(key, text)) {
      if (text.equals("\n")) {
        continue;
      }
      byte[] lineBytes = text.toString().getBytes("UTF-8");
      M tValue = converter_.fromBytes(base64_.decode(lineBytes));
      if (tValue != null) {
        value.set(tValue);
        return true;
      }
    }
    return false;
  }
}
