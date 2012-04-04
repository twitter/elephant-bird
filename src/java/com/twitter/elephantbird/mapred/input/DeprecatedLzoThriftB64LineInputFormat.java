package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.thrift.TBase;

import java.io.IOException;

/**
 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style
 * which is deprecated but still required in places.
 *
 * @author Yifan Shi
 *
 * TODO: should extend DeprecatedInputFormatWrapper (or removed)
 */
@SuppressWarnings("deprecation")
public class DeprecatedLzoThriftB64LineInputFormat<M extends TBase<?, ?>>
    extends DeprecatedLzoInputFormat<LongWritable, ThriftWritable<M>> {

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  public static void setClassConf(Class<? extends TBase<?, ?>> thriftClass, Configuration conf) {
    ThriftUtils.setClassConf(conf, DeprecatedLzoThriftB64LineInputFormat.class, thriftClass);
  }

  /**
   * Return a DeprecatedLzoThriftB64LineRecordReader to handle the work.
   * @throws IOException
   */
  @Override
  public RecordReader<LongWritable, ThriftWritable<M>> getRecordReader(
      InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {

    TypeRef<M> typeRef = ThriftUtils.getTypeRef(jobConf, this.getClass());

    reporter.setStatus(inputSplit.toString());
    return new DeprecatedLzoThriftB64LineRecordReader<M>(
        jobConf, (FileSplit)inputSplit, typeRef);
  }
}
