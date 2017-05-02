package com.twitter.elephantbird.pig.util;

import com.twitter.elephantbird.thrift.test.Name;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

/**
 * @author Andy Schlaikjer
 */
public class TestThriftNameWritableConverter extends
    AbstractTestThriftNameWritableConverter<ThriftWritable<Name>, ThriftWritableConverter<Name>> {
  public TestThriftNameWritableConverter() {
    super(getWritableClass(Name.class, ThriftWritable.class), getWritableConverterClass(Name.class,
        getWritableClass(Name.class, ThriftWritable.class), ThriftWritableConverter.class));
  }
}
