package com.twitter.elephantbird.pig.util;

import com.twitter.elephantbird.thrift.test.Name;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;

/**
 * @author Andy Schlaikjer
 */
public class ThriftNameWritable extends ThriftWritable<Name> {
  public ThriftNameWritable() {
    super(ThriftUtils.<Name>getTypeRef(Name.class));
  }
}
