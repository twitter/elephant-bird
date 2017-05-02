package com.twitter.elephantbird.cascading2.scheme;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

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
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf ) {
    DelegateCombineFileInputFormat.setDelegateInputFormat(conf, LzoTextInputFormat.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf ) {
    conf.setOutputFormat(DeprecatedLzoTextOutputFormat.class);
  }
}
