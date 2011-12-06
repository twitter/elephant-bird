package com.twitter.elephantbird.cascading.scheme;

import com.hadoop.compression.lzo.LzopCodec;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoTextInputFormat;

import cascading.scheme.TextDelimited;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Scheme for LZO encoded TSV files.
 *
 * @author Ning Liang
 */
public class LzoTextDelimited extends TextDelimited {

  public LzoTextDelimited(Fields fields, String delimiter) {
    super(fields, delimiter);
  }

  public LzoTextDelimited(Fields fields, boolean skipHeader, String delimiter) {
    super(fields, skipHeader, delimiter);
  }

  public LzoTextDelimited(Fields fields, String delimiter, Class[] types) {
    super(fields, delimiter, types);
  }

  public LzoTextDelimited(Fields fields, boolean skipHeader, String delimiter, Class[] types) {
    super(fields, skipHeader, delimiter, types);
  }

  public LzoTextDelimited(Fields fields, String delimiter, String quote, Class[] types) {
    super(fields, delimiter, quote, types);
  }

  public LzoTextDelimited(Fields fields, boolean skipHeader, String delimiter,
    String quote, Class[] types) {
    super(fields, skipHeader, delimiter, quote, types);
  }

  public LzoTextDelimited(Fields fields, String delimiter,
    String quote, Class[] types, boolean safe) {
    super(fields, delimiter, quote, types, safe);
  }

  public LzoTextDelimited(Fields fields, boolean skipHeader, String delimiter,
    String quote, Class[] types, boolean safe) {
    super(fields, skipHeader, delimiter, quote, types, safe);
  }

  public LzoTextDelimited(Fields fields, String delimiter, String quote) {
    super(fields, delimiter, quote);
  }

  public LzoTextDelimited(Fields fields, boolean skipHeader, String delimiter, String quote) {
    super(fields, skipHeader, delimiter, quote);
  }

  @Override
  public void sourceInit(Tap tap, JobConf conf) {
    conf.setInputFormat(DeprecatedLzoTextInputFormat.class);
  }

  @Override
  public void sinkInit(Tap tap, JobConf conf) {
    conf.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setCompressOutput(conf, true);
    FileOutputFormat.setOutputCompressorClass(conf, LzopCodec.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  }
}
