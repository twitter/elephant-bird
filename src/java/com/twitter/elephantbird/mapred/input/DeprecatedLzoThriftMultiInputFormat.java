package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
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
 */
@SuppressWarnings("deprecation")
public class DeprecatedLzoThriftMultiInputFormat<M extends TBase<?, ?>>
    extends DeprecatedLzoInputFormat<LongWritable, ThriftWritable<M>> {

  /**
   * Returns DeprecatedLzoThriftB64LineInputFormat class for setting up a job.
   * Sets an internal configuration in jobConf so that Task instantiates
   * appropriate object for this generic class based on thriftClass
   */
  //@SuppressWarnings("unchecked")
  public static <M extends TBase<?, ?>> Class<DeprecatedLzoThriftMultiInputFormat>
     getInputFormatClass(Class<M> thriftClass, Configuration jobConf) {
    return getInputFormatClass(
        DeprecatedLzoThriftMultiInputFormat.class, thriftClass, jobConf);
  }

  /**
   * Sets an internal configuration in jobConf so that Task instantiates
   * appropriate object for this generic class based on thriftClass.
   * Returns formatClass.
   */
  public static <T extends InputFormat, M extends TBase<?, ?>> Class<T> getInputFormatClass(
      Class<T> formatClass, Class<M> thriftClass, Configuration jobConf) {
    ThriftUtils.setClassConf(jobConf, formatClass, thriftClass);
    return formatClass;
  }


  /**
   * Return a DeprecatedLzoThriftB64LineRecordReader to handle the work.
   * @throws IOException
   */
  @Override
  public RecordReader getRecordReader(
      InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {

    TypeRef<M> typeRef = ThriftUtils.getTypeRef(jobConf, this.getClass());

    reporter.setStatus(inputSplit.toString());

    MultiInputFormat.Format format =
      MultiInputFormat.determineFileFormat(((FileSplit)inputSplit).getPath(), jobConf);

    switch (format) {
    case LZO_B64LINE:
      return new DeprecatedLzoThriftB64LineRecordReader<M>(
          jobConf, (FileSplit)inputSplit, typeRef);
    case LZO_BLOCK:
      return new DeprecatedLzoThriftBlockRecordReader<M>(typeRef, jobConf,
          (FileSplit)inputSplit);
    }
    // not expected.
    throw new IOException("could not determine file format");
  }
}
