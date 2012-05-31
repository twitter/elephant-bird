package com.twitter.elephantbird.pig.util;

import java.lang.reflect.Array;

import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * @author Andy Schlaikjer
 */
public abstract class AbstractTestThriftNameWritableConverter<W extends ThriftWritable<Name>, C extends ThriftWritableConverter<Name>>
    extends AbstractTestThriftWritableConverter<Name> {
  public static final TypeRef<Name> TYPE_REF = ThriftUtils.getTypeRef(Name.class);
  private static final Name V1 = new Name("Jon", "Smith");
  private static final Name V2 = new Name("John", "Doe");
  private static final Name V3 = new Name("Mary", "Jane");
  private static final String[] EXPECTED = { "(Jon,Smith)", "(John,Doe)", "(Mary,Jane)" };

  public AbstractTestThriftNameWritableConverter(Class<W> writableClass,
      Class<C> writableConverterClass) {
    super(Name.class, writableClass, writableConverterClass, getData(writableClass), EXPECTED,
        "tuple()");
  }

  protected static <W extends ThriftWritable<Name>> W[] getData(Class<W> writableClass) {
    try {
      @SuppressWarnings("unchecked")
      W[] ws = (W[]) Array.newInstance(writableClass, 3);
      ws[0] = writableClass.newInstance();
      ws[1] = writableClass.newInstance();
      ws[2] = writableClass.newInstance();
      ws[0].setConverter(Name.class);
      ws[1].setConverter(Name.class);
      ws[2].setConverter(Name.class);
      ws[0].set(V1);
      ws[1].set(V2);
      ws[2].set(V3);
      return ws;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
