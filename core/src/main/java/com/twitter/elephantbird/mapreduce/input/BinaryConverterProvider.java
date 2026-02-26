package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import org.apache.hadoop.conf.Configuration;


/**
 * A simple interface to serialize and deserialize objects
 */
public interface BinaryConverterProvider<M> {
  BinaryConverter<M> getConverter(Configuration conf);
}
