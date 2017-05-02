package com.twitter.elephantbird.pig.util;

import java.nio.ByteBuffer;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.twitter.elephantbird.thrift.test.Name;
import com.twitter.elephantbird.thrift.test.Person;
import com.twitter.elephantbird.thrift.test.PhoneNumber;
import com.twitter.elephantbird.thrift.test.PhoneType;
import com.twitter.elephantbird.pig.test.thrift.KeyEnum;
import com.twitter.elephantbird.pig.test.thrift.MapKeyTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link PigToThrift}.
 *
 * @author Andy Schlaikjer
 */
public class TestPigToThrift {
  private static final TupleFactory TF = TupleFactory.getInstance();

  private static Tuple tuple(Object... values) {
    return TF.newTupleNoCopy(Lists.newArrayList(values));
  }

  private static Person personMessage(String name, int id, String email, String phoneNumber,
      String phoneType) {
    return new Person(new Name(name, null), id, email, Lists.newArrayList(new PhoneNumber(
        phoneNumber).setType(PhoneType.valueOf(phoneType))));
  }

  private static Tuple personTuple(String name, int id, String email, String phoneNumber,
      String phoneType) {
    return tuple(tuple(name, null), id, email,
        new NonSpillableDataBag(Lists.<Tuple>newArrayList(tuple(phoneNumber, phoneType))));
  }

  @Test
  public void testPerson() {
    Person expected = personMessage("Joe", 1, null, "123-456-7890", "HOME");
    Person actual = PigToThrift.newInstance(Person.class).getThriftObject(
        personTuple("Joe", 1, null, "123-456-7890", "HOME"));
    assertNotNull(actual);
    assertEquals(expected, actual);
  }

  @Test//(expected = RuntimeException.class)
  public void testPersonBadEnumValue() {
    PigToThrift.newInstance(Person.class).getThriftObject(
        personTuple("Joe", 1, null, "123-456-7890", "ASDF"));
  }

  @Test
  public void testSupportedMapKeyTypes() {
    MapKeyTest expected = new MapKeyTest()
        .setBooleans(ImmutableMap.of(true, 1))
        .setBytes(ImmutableMap.of((byte) 1, 1))
        .setShorts(ImmutableMap.of((short) 1, 1))
        .setInts(ImmutableMap.of(1, 1))
        .setLongs(ImmutableMap.of(1L, 1))
        .setDoubles(ImmutableMap.of(1D, 1))
        .setEnums(ImmutableMap.of(KeyEnum.A, 1))
        .setStrings(ImmutableMap.of("a", 1))
        .setBinaries(ImmutableMap.of(ByteBuffer.wrap("1".getBytes(Charsets.UTF_8)), 1));
    MapKeyTest actual = PigToThrift.newInstance(MapKeyTest.class).getThriftObject(
        tuple(
            ImmutableMap.of("true", 1),
            ImmutableMap.of("1", 1),
            ImmutableMap.of("1", 1),
            ImmutableMap.of("1", 1),
            ImmutableMap.of("1", 1),
            ImmutableMap.of("1.0", 1),
            ImmutableMap.of(KeyEnum.A.name(), 1),
            ImmutableMap.of("a", 1),
            ImmutableMap.of("1", 1)
        )
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testMapKeyConversionFailure() {
    MapKeyTest fail = PigToThrift.newInstance(MapKeyTest.class).getThriftObject(
        tuple(
            null,
            ImmutableMap.of("notabyte", 1)
        )
    );
    assertNotNull(fail);
    assertNull(fail.getBytes());
  }
}
