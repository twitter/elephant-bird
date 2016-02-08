package com.twitter.elephantbird.cascading3.scheme;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;

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

  public LzoTextDelimited(Fields fields, boolean skipHeader, boolean writeHeader, String delimiter,
    boolean strict, String quote, Class[] types, boolean safe) {
    // We set Compress to null as this class's point is to explicitly handle this
    super(fields, null, skipHeader, writeHeader, delimiter, strict, quote, types, safe);
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
  public void sourceConfInit(FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf ) {
    conf.setClass("mapred.input.format.class", LzoTextInputFormat.class, InputFormat.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf ) {
    conf.setClass("mapred.output.format.class", DeprecatedLzoTextOutputFormat.class, OutputFormat.class);
  }
}
