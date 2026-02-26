package com.twitter.elephantbird.cascading2.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MapReduceInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.util.TempHfs;
import cascading.tuple.Fields;
import static org.junit.Assert.assertEquals;

public class TestCombinedSequenceFile {

  @Test
  public void testHadoopConf() {
    CombinedSequenceFile csfScheme = new CombinedSequenceFile(Fields.ALL);
    JobConf conf = new JobConf();
    FlowProcess fp = new HadoopFlowProcess();
    Tap<JobConf, RecordReader, OutputCollector> tap =
        new TempHfs(conf, "test", CombinedSequenceFile.class, false);

    csfScheme.sourceConfInit(fp, tap, conf);

    assertEquals(
        "MapReduceInputFormatWrapper shold wrap mapred.SequenceFileinputFormat",
        "org.apache.hadoop.mapred.SequenceFileInputFormat",
        conf.get(MapReduceInputFormatWrapper.CLASS_CONF_KEY)
    );
    assertEquals(
        "Delegate combiner should wrap MapReduceInputFormatWrapper",
        "com.twitter.elephantbird.mapreduce.input.MapReduceInputFormatWrapper",
        conf.get(DelegateCombineFileInputFormat.COMBINED_INPUT_FORMAT_DELEGATE)
    );
    assertEquals(
        "DeprecatedInputFormatWrapper should wrap Delegate combiner",
        "com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat",
        conf.get(DeprecatedInputFormatWrapper.CLASS_CONF_KEY)
    );
  }

}
