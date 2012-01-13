package com.twitter.elephantbird.cascading2.scheme;

import com.hadoop.compression.lzo.LzopCodec;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.twitter.elephantbird.mapred.input.DeprecatedLzoTextInputFormat;

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
    conf.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setCompressOutput(conf, true);
    FileOutputFormat.setOutputCompressorClass(conf, LzopCodec.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  }
}
