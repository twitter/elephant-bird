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

  private static final String MR_COMPRESS_ENABLE = "mapreduce.output.fileoutputformat.compress";
  public static final String COMPRESS_ENABLE = "elephantbird.cascading.combinedsequencefile.compress.enable";

  private static final String MR_COMPRESS_TYPE = "mapreduce.output.fileoutputformat.compress.type";
  public static final String COMPRESS_TYPE = "elephantbird.cascading.combinedsequencefile.compress.type";

  private static final String MR_COMPRESS_CODEC = "mapreduce.output.fileoutputformat.compress.codec";
  public static final String COMPRESS_CODEC = "elephantbird.cascading.combinedsequencefile.compress.codec";


  protected CombinedSequenceFile() { super(); }

  public CombinedSequenceFile(Fields fields) { super(fields); }

  // We can allow overriding the compression settings for just this scheme here
  private void updateJobConfForLocalSettings(JobConf conf) {
    String localSetCompressionEnabled = conf.get(COMPRESS_ENABLE);
    if(localSetCompressionEnabled != null) {
      conf.set(MR_COMPRESS_ENABLE, localSetCompressionEnabled);
    }

    String localSetCompressionType = conf.get(COMPRESS_TYPE);
    if(localSetCompressionType != null) {
      conf.set(MR_COMPRESS_TYPE, localSetCompressionType);
    }

    String localSetCompressionCodec = conf.get(COMPRESS_CODEC);
    if(localSetCompressionCodec != null) {
      conf.set(MR_COMPRESS_CODEC, localSetCompressionCodec);
    }
  }

  @Override
  public void sourceConfInit(
      FlowProcess<JobConf> flowProcess,
      Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf ) {
    super.sourceConfInit(flowProcess, tap, conf);

    updateJobConfForLocalSettings(conf);

    // Since the EB combiner works over the mapreduce API while Cascading is on the mapred API,
    // in order to use the EB combiner we must wrap the mapred SequenceFileInputFormat
    // with the MapReduceInputFormatWrapper, then wrap the DelegateCombineFileInputFormat
    // again with DeprecatedInputFormatWrapper to make it compatible with Cascading.
    MapReduceInputFormatWrapper.setWrappedInputFormat(SequenceFileInputFormat.class, conf);
    DelegateCombineFileInputFormat.setDelegateInputFormat(conf, MapReduceInputFormatWrapper.class);
  }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
  {
    super.sinkConfInit(flowProcess, tap, conf);

    updateJobConfForLocalSettings(conf);
  }

}
