package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import com.twitter.elephantbird.mapreduce.input.MapReduceInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * This scheme allows SequenceFile splits to be combined via the DelegateCombineFileInputFormat
 * before it is read. It can be used to combine inputs for intermediate MR jobs in Cascading.
 *
 * To enable, set cascading.flowconnector.intermediateschemeclass to this class in the Hadoop
 * configuration.
 *
 * @author Akihiro Matsukawa
 */
public class CombinedSequenceFile extends SequenceFile {

  protected CombinedSequenceFile() { super(); }

  public CombinedSequenceFile(Fields fields) { super(fields); }

  @Override
  public void sourceConfInit(
      FlowProcess<JobConf> flowProcess,
      Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf ) {
    super.sourceConfInit(flowProcess, tap, conf);

    // Since the EB combiner works over the mapreduce API while Cascading is on the mapred API,
    // in order to use the EB combiner we must wrap the mapred SequenceFileInputFormat
    // with the MapReduceInputFormatWrapper, then wrap the DelegateCombineFileInputFormat
    // again with DeprecatedInputFormatWrapper to make it compatible with Cascading.
    MapReduceInputFormatWrapper.setWrappedInputFormat(SequenceFileInputFormat.class, conf);
    DelegateCombineFileInputFormat.setDelegateInputFormat(conf, MapReduceInputFormatWrapper.class);
  }

}
