package com.twitter.elephantbird.pig.load;

import org.apache.thrift.TBase;

/**
 * @Deprecated use {@link ThriftPigLoader}
 */
public class LzoThriftB64LinePigLoader<M extends TBase<?, ?>> extends ThriftPigLoader<M> {

  public LzoThriftB64LinePigLoader(String thriftClassName) {
    super(thriftClassName);
    LOG.warn("LzoThriftB64LinePigLoader is deprecated and will be removed in future. " +
             "please use ThriftPigLoader");
  }
}
