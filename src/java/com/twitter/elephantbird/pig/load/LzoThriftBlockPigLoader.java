package com.twitter.elephantbird.pig.load;

import org.apache.thrift.TBase;

public class LzoThriftBlockPigLoader<M extends TBase<?, ?>> extends LzoThriftB64LinePigLoader<M> {

  public LzoThriftBlockPigLoader(String thriftClassName) {
    super(thriftClassName);
  }
}
