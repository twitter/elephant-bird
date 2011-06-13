package com.twitter.elephantbird.mapred.output;

import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TBase;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A RecordWriter-derived class for use with the DeprecatedLzoThriftB64LineOutputFormat.
 * Writes data as lzo compressed base64 encoded serialized string, one per line.
 *
 * @author Yifan Shi
 */
public class DeprecatedLzoThriftB64LineRecordWriter<M extends TBase<?, ?>>
    implements RecordWriter<NullWritable, ThriftWritable<M>> {

  private final ThriftConverter<M> converter_;
  private final DataOutputStream out_;
  private final Base64 base64_;

  public DeprecatedLzoThriftB64LineRecordWriter(ThriftConverter<M> converter, DataOutputStream out) {
    converter_ = converter;
    out_ = out;
    base64_ = Codecs.createStandardBase64();
  }

  public void write(NullWritable nullWritable, ThriftWritable<M> writable) throws IOException {
    byte[] b64Bytes = base64_.encode(converter_.toBytes(writable.get()));
    out_.write(b64Bytes);
    out_.write(Protobufs.NEWLINE_UTF8_BYTE);
  }

  public void close(Reporter reporter) throws IOException {
    out_.close();
  }
}
