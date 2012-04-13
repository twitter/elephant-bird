package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Hive-specific wrapper around {@link MultiInputFormat}. This is necessary to set the
 * {@link TypeRef} because Hive does not support InputFormat constructor arguments.
 * This pairs well with {@link com.twitter.elephantbird.hive.serde.ThriftSerDe}.
 */
@SuppressWarnings("deprecation")
public class HiveMultiInputFormat
    extends DeprecatedInputFormatWrapper<LongWritable, BinaryWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMultiInputFormat.class);

  public HiveMultiInputFormat() {
    super(new MultiInputFormat());
  }

  private void initialize(FileSplit split, JobConf job) throws IOException {
    LOG.info("Initializing HiveMultiInputFormat for " + split + " with job " + job);

    String thriftClassName = null;
    Properties properties = null;

    if (!"".equals(HiveConf.getVar(job, HiveConf.ConfVars.PLAN))) {
      // Running as a Hive query. Use MapredWork for metadata.
      Map<String, PartitionDesc> partitionDescMap =
          Utilities.getMapRedWork(job).getPathToPartitionInfo();

      if (!partitionDescMap.containsKey(split.getPath().getParent().toUri().toString())) {
        throw new RuntimeException("Failed locating partition description for "
            + split.getPath().toUri().toString());
      }
      properties = partitionDescMap.get(split.getPath().getParent().toUri().toString())
          .getTableDesc().getProperties();
    } else if (job.get(HCatConstants.HCAT_KEY_JOB_INFO, null) != null) {
      // Running as an HCatalog query. Use InputJobInfo for metadata.
      InputJobInfo inputJobInfo = (InputJobInfo) HCatUtil.deserialize(
          job.get(HCatConstants.HCAT_KEY_JOB_INFO));
      properties = inputJobInfo.getTableInfo().getStorerInfo().getProperties();
    }

    if (properties != null) {
      thriftClassName = properties.getProperty(Constants.SERIALIZATION_CLASS);
    }

    if (thriftClassName == null) {
      throw new RuntimeException(
          "Required property " + Constants.SERIALIZATION_CLASS + " is null.");
    }

    try {
      Class thriftClass = job.getClassByName(thriftClassName);
      realInputFormat = new MultiInputFormat(new TypeRef(thriftClass) {});
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed getting class for " + thriftClassName);
    }
  }

  public RecordReader<LongWritable, BinaryWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    initialize((FileSplit) split, job);
    return super.getRecordReader(split, job, reporter);
  }
}
