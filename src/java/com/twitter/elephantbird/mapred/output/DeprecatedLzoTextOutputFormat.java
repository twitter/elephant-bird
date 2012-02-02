package com.twitter.elephantbird.mapred.output;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.LzoUtils;

@Deprecated
public class DeprecatedLzoTextOutputFormat<K, V> extends TextOutputFormat<K, V> {

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {

    Path file = getPathForCustomFile(job,  "part");
    file = file.suffix(LzopCodec.DEFAULT_LZO_EXTENSION);

    return new LineRecordWriter<K, V>(
                  LzoUtils.getIndexedLzoOutputStream(job, file),
                  job.get("mapred.textoutputformat.separator", "\t"));
  }

}
