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
 *
 * TODO:
 *
 * Hive's RCFileOutputFormat ported to non-deprecated mapreduce + Metadata support
 */

public class RCFileOutputFormat extends FileOutputFormat<NullWritable, Writable> {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileOutputFormat.class);

  // in case we need different compression from global default compression
  public static String RCFILE_COMPRESSION_CODEC_CONF = "elephantbird.rcfile.output.compression.codec";

  public static String RCFILE_DEFAULT_EXTENSION = "rc";
  public static String RCFILE_EXTENSION_OVERRIDE_CONF = "elephantbird.refile.output.filename.extension"; // "none" disables it.

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


  @Override
  public RecordWriter<NullWritable, Writable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {

    Configuration conf = job.getConfiguration();

    // override compression codec if set.
    String codecOverride = conf.get(RCFILE_COMPRESSION_CODEC_CONF);
    if (codecOverride != null) {
      conf.setBoolean("mapred.output.compress", true);
      conf.set("mapred.output.compression.codec", codecOverride);
    }

    CompressionCodec codec = null;
    if (getCompressOutput(job)) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    String ext = conf.get(RCFILE_EXTENSION_OVERRIDE_CONF, RCFILE_DEFAULT_EXTENSION);
    Path file = getDefaultWorkFile(job, ext.equalsIgnoreCase("none") ? null : ext);

    LOG.info("writing to rcfile " + file.toString());

    // TODO : add metadata.
    final RCFile.Writer out = new RCFile.Writer(file.getFileSystem(conf), conf, file, job, codec);

    return new RecordWriter<NullWritable, Writable>() {
      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
      }

      @Override
      public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
        out.append(value);
      }
    };

  }
}
