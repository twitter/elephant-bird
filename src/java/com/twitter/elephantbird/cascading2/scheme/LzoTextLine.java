package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoTextInputFormat;
import com.twitter.elephantbird.mapred.output.DeprecatedLzoTextOutputFormat;

import cascading.flow.hadoop.HadoopFlowProcess;
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
  public void sourceConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
    conf.setInputFormat(DeprecatedLzoTextInputFormat.class);
  }

  @Override
  public void sinkConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
    conf.setOutputFormat(DeprecatedLzoTextOutputFormat.class);
  }
}
