package com.twitter.elephantbird.pig.util;

import com.twitter.elephantbird.thrift.test.Name;

/**
 * @author Andy Schlaikjer
 */
public class TestThriftNameWritableConverterCustom extends
    AbstractTestThriftNameWritableConverter<ThriftNameWritable, ThriftWritableConverter<Name>> {
  public TestThriftNameWritableConverterCustom() {
    super(ThriftNameWritable.class, getWritableConverterClass(Name.class, ThriftNameWritable.class,
        ThriftWritableConverter.class));
  }
}
