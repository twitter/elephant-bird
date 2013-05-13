package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.LzoUtils;

public class LzoTextOutputFormat<K, V> extends TextOutputFormat<K, V>  {

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {

    Configuration conf = HadoopCompat.getConfiguration(job);
    Path path = getDefaultWorkFile(job, LzopCodec.DEFAULT_LZO_EXTENSION);

    return new LineRecordWriter<K, V>(
                   LzoUtils.getIndexedLzoOutputStream(conf, path),
                   conf.get("mapred.textoutputformat.separator", "\t")
                   );
  }

}
