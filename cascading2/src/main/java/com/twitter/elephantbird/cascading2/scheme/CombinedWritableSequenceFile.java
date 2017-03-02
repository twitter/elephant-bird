package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class CombinedWritableSequenceFile extends WritableSequenceFile {
  public CombinedWritableSequenceFile(Fields fields, Class<? extends Writable> valueType) {
    super(fields, valueType);
  }

  public CombinedWritableSequenceFile(Fields fields, Class<? extends Writable> keyType, Class<? extends Writable> valueType) {
    super(fields, keyType, valueType);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    super.sourceConfInit(flowProcess, tap, conf);

    CombinedSequenceFile.sourceConfInit(conf);
  }
}
