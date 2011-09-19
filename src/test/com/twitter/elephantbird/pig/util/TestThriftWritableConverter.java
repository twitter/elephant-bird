package com.twitter.elephantbird.pig.util;

import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * @author Andy Schlaikjer
 */
public class TestThriftWritableConverter extends AbstractTestThriftWritableConverter<Name> {
  private static final TypeRef<Name> TYPE_REF = ThriftUtils.getTypeRef(Name.class);
  private static final Name V1 = new Name("Jon", "Smith");
  private static final Name V2 = new Name("John", "Doe");
  private static final Name V3 = new Name("Mary", "Jane");
  private static final ThriftWritable<?>[] DATA = { new ThriftWritable<Name>(V1, TYPE_REF),
          new ThriftWritable<Name>(V2, TYPE_REF), new ThriftWritable<Name>(V3, TYPE_REF) };
  private static final String[] EXPECTED = { "(Jon,Smith)", "(John,Doe)", "(Mary,Jane)" };

  @SuppressWarnings("unchecked")
  public TestThriftWritableConverter() {
    super(Name.class, (ThriftWritable<Name>[]) DATA, EXPECTED, "tuple()");
  }
}
