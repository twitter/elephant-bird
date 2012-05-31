package com.twitter.elephantbird.pig.util;

import com.twitter.data.proto.tutorial.thrift.Name;

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
