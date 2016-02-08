/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.elephantbird.cascading3.scheme;

import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.RawBytesWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryBlockOutputFormat;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;

/**
 * Scheme for lzo compressed files with binary records.
 *
 * @author Sam Ritchie
 */
public class LzoByteArrayScheme extends LzoBinaryScheme<byte[], RawBytesWritable> {
  @Override protected RawBytesWritable prepareBinaryWritable() {
    return new RawBytesWritable();
  }

  @Override public void sourceConfInit(FlowProcess<? extends Configuration> fp,
      Tap<Configuration, RecordReader, OutputCollector> tap,
      Configuration conf) {
    MultiInputFormat.setClassConf(byte[].class, conf);
    DelegateCombineFileInputFormat.setDelegateInputFormatHadoop2(conf, MultiInputFormat.class);
  }

  @Override public void sinkConfInit(FlowProcess<? extends Configuration> fp,
      Tap<Configuration, RecordReader, OutputCollector> tap,
      Configuration conf) {
    conf.setClass("mapreduce.outputformat.class", LzoBinaryBlockOutputFormat.class, OutputFormat.class);
  }
}
