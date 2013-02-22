package com.twitter.elephantbird.pig.load;

import org.apache.thrift.TBase;

/**
 * @Deprecated use {@link ThriftPigLoader}
 */
public class LzoThriftBlockPigLoader<M extends TBase<?, ?>> extends ThriftPigLoader<M> {

  public LzoThriftBlockPigLoader(String thriftClassName) {
    super(thriftClassName);
    LOG.warn("LzoThriftBlockPigLoader is deprecated and will be removed in future " +
             "please use ThriftPigLoader");
 }
}
