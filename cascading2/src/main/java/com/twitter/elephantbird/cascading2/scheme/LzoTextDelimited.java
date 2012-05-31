package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoTextInputFormat;
import com.twitter.elephantbird.mapred.output.DeprecatedLzoTextOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextDelimited;
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
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf ) {
    conf.setInputFormat(DeprecatedLzoTextInputFormat.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf ) {
    conf.setOutputFormat(DeprecatedLzoTextOutputFormat.class);
  }
}
