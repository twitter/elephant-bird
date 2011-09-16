package com.twitter.elephantbird.pig.util;

import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.pig.util.TestThriftWritableConverter.NameWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * @author Andy Schlaikjer
 */
public class TestThriftWritableConverter extends
    AbstractTestThriftWritableConverter<Name, NameWritable> {
  /**
   * @author Andy Schlaikjer
   */
  public static class NameWritable extends ThriftWritable<Name> {
    public NameWritable() {
      super(TYPE_REF);
    }

    public NameWritable(Name value) {
      super(value, TYPE_REF);
    }
  }

  private static final TypeRef<Name> TYPE_REF = ThriftUtils.getTypeRef(Name.class);
  private static final Name V1 = new Name("Jon", "Smith");
  private static final Name V2 = new Name("John", "Doe");
  private static final Name V3 = new Name("Mary", "Jane");
  private static final NameWritable[] DATA = { new NameWritable(V1), new NameWritable(V2),
          new NameWritable(V3) };
  private static final String[] EXPECTED = { "(Jon,Smith)", "(John,Doe)", "(Mary,Jane)" };

  public TestThriftWritableConverter() {
    super(Name.class, NameWritable.class, DATA, EXPECTED, "tuple()");
  }
}
