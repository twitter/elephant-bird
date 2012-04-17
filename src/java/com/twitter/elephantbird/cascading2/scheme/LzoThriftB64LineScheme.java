package com.twitter.elephantbird.cascading2.scheme;

import org.apache.thrift.TBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Scheme for Thrift B64 encoded files.
 * @deprecated please use {@link LzoThriftScheme}
 * @author Argyris Zymnis
 */
@Deprecated
public class LzoThriftB64LineScheme<M extends TBase<?,?>> extends
  LzoThriftScheme<M> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoThriftB64LineScheme.class);
  public LzoThriftB64LineScheme(Class thriftClass) {
    super(thriftClass);
    LOG.warn("LzoThriftB64LineScheme is deprecated, please use LzoThriftScheme");
  }
}
