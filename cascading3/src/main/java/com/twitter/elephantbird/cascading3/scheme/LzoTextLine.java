package com.twitter.elephantbird.cascading3.scheme;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;

import com.twitter.elephantbird.mapred.output.DeprecatedLzoTextOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Scheme for LZO encoded text files.
 *
 * @author Ning Liang
 */
public class LzoTextLine extends TextLine {

  public LzoTextLine() {
    super();
  }

  public LzoTextLine(int numSinkParts) {
    super(numSinkParts);
  }

  public LzoTextLine(Fields sourceFields, Fields sinkFields) {
    super(sourceFields, sinkFields);
  }

  public LzoTextLine(Fields sourceFields, Fields sinkFields, int numSinkParts) {
    super(sourceFields, sinkFields, numSinkParts);
  }

  public LzoTextLine(Fields sourceFields) {
    super(sourceFields);
  }

  public LzoTextLine(Fields sourceFields, int numSinkParts) {
    super(sourceFields, numSinkParts);
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf ) {
    DeprecatedInputFormatWrapper.setInputFormat(LzoTextInputFormat.class, conf);
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf ) {
    conf.setClass("mapred.output.format.class", DeprecatedLzoTextOutputFormat.class, OutputFormat.class);
  }
}
