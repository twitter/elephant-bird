package com.twitter.elephantbird.cascading2.scheme;

import org.apache.thrift.TBase;

import java.io.IOException;

/**
 * Scheme for Thrift B64 encoded files.
 * @deprecated please use {@link LzoThriftScheme}
 * @author Argyris Zymnis
 */
@Deprecated
public class LzoThriftB64LineScheme<M extends TBase<?,?>> extends
  LzoThriftScheme<M> {
  public LzoThriftB64LineScheme(Class thriftClass) {
    super(thriftClass);
  }
}
