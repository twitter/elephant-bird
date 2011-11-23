package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive's {@link org.apache.hadoop.hive.ql.io.RCFileOutputFormat} is written for
 * deprecated OutputFormat. Pig requires newer OutputFormat.
 * In addition RCFileOutputFormat's functionality this class adds RCFile
 * metadata support.
 *
 * TODO: contribute this to PIG.
 */

public class RCFileOutputFormat extends FileOutputFormat<NullWritable, Writable> {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileOutputFormat.class);

  // in case we need different compression from global default compression
  public static String COMPRESSION_CODEC_CONF = "elephantbird.rcfile.output.compression.codec";

  public static String DEFAULT_EXTENSION = ".rc";
  public static String EXTENSION_OVERRIDE_CONF = "elephantbird.refile.output.filename.extension"; // "none" disables it.

  public static String COLUMN_METADATA_PROTOBUF_KEY = "elephantbird.rcfile.column.info.protobuf";

  /**
   * set number of columns into the given configuration.
   *
   * @param conf
   *          configuration instance which need to set the column number
   * @param columnNum
   *          column number for RCFile's Writer
   *
   */
  public static void setColumnNumber(Configuration conf, int columnNum) {
    assert columnNum > 0;
    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
  }

  /**
   * Returns the number of columns set in the conf for writers.
   *
   * @param conf
   * @return number of columns for RCFile's writer
   */
  public static int getColumnNumber(Configuration conf) {
    return conf.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
  }

  public void setColumnInfo(){}
  protected RCFile.Writer createRCFileWriter(TaskAttemptContext job) throws IOException {
    Configuration conf = job.getConfiguration();

    // override compression codec if set.
    String codecOverride = conf.get(COMPRESSION_CODEC_CONF);
    if (codecOverride != null) {
      conf.setBoolean("mapred.output.compress", true);
      conf.set("mapred.output.compression.codec", codecOverride);
    }

    CompressionCodec codec = null;
    if (getCompressOutput(job)) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    String ext = conf.get(EXTENSION_OVERRIDE_CONF, DEFAULT_EXTENSION);
    Path file = getDefaultWorkFile(job, ext.equalsIgnoreCase("none") ? null : ext);

    LOG.info("writing to rcfile " + file.toString());

    // TODO add metadata
    return new RCFile.Writer(file.getFileSystem(conf), conf, file, job, codec);
  }

  /**
   * RecordWriter wrapper around an RCFile.Writer
   */
  static protected class Writer extends RecordWriter<NullWritable, Writable> {

    private RCFile.Writer rcfile;

    protected Writer(RCFileOutputFormat outputFormat, TaskAttemptContext job) throws IOException {
      rcfile = outputFormat.createRCFileWriter(job);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      rcfile.close();
    }

    @Override
    public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
      rcfile.append(value);
      // add counters
    }
  }

  @Override
  public RecordWriter<NullWritable, Writable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    return new Writer(this, job);
  }
}
